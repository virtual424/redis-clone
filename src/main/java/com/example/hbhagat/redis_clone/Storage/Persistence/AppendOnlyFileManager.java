package com.example.hbhagat.redis_clone.Storage.Persistence;

import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Config.RedisProperties;
import com.example.hbhagat.redis_clone.Protocol.RespArray;
import com.example.hbhagat.redis_clone.Protocol.RespBulkString;
import com.example.hbhagat.redis_clone.Protocol.RespValue;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.example.hbhagat.redis_clone.Storage.Core.RedisObject;
import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import com.example.hbhagat.redis_clone.Storage.Mutation.Mutation;
import com.example.hbhagat.redis_clone.Storage.Mutation.MutationResult;
import org.springframework.stereotype.Component;

@Component
public final class AppendOnlyFileManager {

	private final RedisProperties properties;

	private final RedisStore store;

	/*
	 * This lock protects the complete mutation + AOF
	 * append sequence and AOF rewrite cut-over.
	 *
	 * All Redis mutations coordinated through
	 * MutationCoordinator eventually pass through this lock.
	 */
	private final ReentrantLock mutationLock = new ReentrantLock(true);

	/*
	 * Only one rewrite can execute at a time.
	 */
	private final AtomicBoolean rewriting = new AtomicBoolean(false);

	/*
	 * Mutations which happen after the rewrite snapshot
	 * has been created are temporarily stored here.
	 *
	 * They are appended to the rewritten AOF before
	 * the rewritten file replaces the original AOF.
	 */
	private final Object rewriteBufferLock = new Object();

	private final List<RedisCommand> rewriteBuffer = new ArrayList<>();

	/*
	 * Rewrite is intentionally performed on a single
	 * background thread.
	 */
	/*
	 * True while the AOF is being replayed at startup.
	 * Replayed commands change memory but must not be
	 * appended to the file a second time.
	 */
	private volatile boolean loading = false;

	private final ExecutorService rewriteExecutor = Executors.newSingleThreadExecutor(runnable -> {
		Thread thread = new Thread(runnable, "aof-rewrite");

		thread.setDaemon(true);

		return thread;
	});

	public AppendOnlyFileManager(RedisProperties properties, RedisStore store) {
		this.properties = properties;
		this.store = store;
	}

	/**
	 * Executes a mutation under the global mutation lock.
	 *
	 * The supplier is responsible only for changing the
	 * in-memory state and returning the logical commands
	 * representing that state change.
	 *
	 * AOF persistence is handled here.
	 *
	 * Command handlers never call AOFManager directly.
	 */
	public MutationResult executeMutation(Supplier<Mutation> mutationSupplier) {
		if (mutationSupplier == null) {
			return MutationResult.empty();
		}

		mutationLock.lock();

		try {
			Mutation mutation = mutationSupplier.get();

			if (mutation == null || mutation.isEmpty()) {
				return MutationResult.empty();
			}

			appendMutationInternal(mutation);

			return new MutationResult(mutation);
		} finally {
			mutationLock.unlock();
		}
	}

	/**
	 * Persists a mutation to the active AOF.
	 *
	 * This method MUST only be called while mutationLock
	 * is held.
	 */
	private void appendMutationInternal(Mutation mutation) {
		if (mutation == null || mutation.isEmpty() || loading) {
			return;
		}

		List<RedisCommand> commands = mutation.commands();

		/*
		 * First append to the active AOF.
		 */
		appendCommands(commands);

		/*
		 * If a background rewrite is running,
		 * these commands occurred after the snapshot.
		 *
		 * They therefore also need to be appended
		 * to the rewritten AOF before the final file swap.
		 */
		if (rewriting.get()) {
			synchronized (rewriteBufferLock) {
				rewriteBuffer.addAll(commands);
			}
		}
	}

	/**
	 * Appends commands to the active AOF file.
	 */
	private void appendCommands(List<RedisCommand> commands) {
		if (commands == null || commands.isEmpty()) {
			return;
		}

		Path aofPath = Path.of(properties.getAofFile());

		try {
			Path parent = aofPath.toAbsolutePath().getParent();

			if (parent != null) {
				Files.createDirectories(parent);
			}

			try (
				OutputStream output = Files.newOutputStream(
					aofPath,
					StandardOpenOption.CREATE,
					StandardOpenOption.WRITE,
					StandardOpenOption.APPEND
				)
			) {
				for (RedisCommand command : commands) {
					output.write(commandToResp(command));
				}

				output.flush();
			}
		} catch (IOException e) {
			throw new IllegalStateException("Unable to write AOF file: " + aofPath, e);
		}
	}

	/**
	 * Converts one logical Redis command into RESP.
	 *
	 * Example:
	 *
	 * SET k1 v1
	 *
	 * becomes:
	 *
	 * *3
	 * $3
	 * SET
	 * $2
	 * k1
	 * $2
	 * v1
	 */
	private byte[] commandToResp(RedisCommand command) {
		List<RespValue> values = new ArrayList<>();

		values.add(new RespBulkString(command.name().getBytes(StandardCharsets.UTF_8)));

		values.addAll(command.arguments());

		return new RespArray(values).encode();
	}

	/**
	 * Called by AofLoader around the startup replay.
	 */
	public void beginLoading() {
		loading = true;
	}

	public void endLoading() {
		loading = false;
	}

	public boolean isLoading() {
		return loading;
	}

	/**
	 * Starts a background AOF rewrite.
	 *
	 * If a rewrite is already running, this call is ignored.
	 */
	public void rewrite() {
		mutationLock.lock();

		try {
			/*
			 * Only one rewrite is allowed.
			 */
			if (!rewriting.compareAndSet(false, true)) {
				return;
			}

			synchronized (rewriteBufferLock) {
				rewriteBuffer.clear();
			}

			/*
			 * Snapshot must be created while mutationLock
			 * is held so no mutation can occur while we
			 * inspect the current keyspace.
			 */
			List<RedisCommand> snapshot = snapshotStore();

			rewriteExecutor.submit(() -> performRewrite(snapshot));
		} finally {
			mutationLock.unlock();
		}
	}

	/**
	 * Creates a logical SET-based representation
	 * of the current Redis state.
	 *
	 * GET is never used here because GET updates
	 * LRU information.
	 */
	private List<RedisCommand> snapshotStore() {
		List<RedisCommand> snapshot = new ArrayList<>();

		for (var entry : store.entriesSnapshot()) {
			String key = entry.getKey();

			RedisObject object = entry.getValue();

			if (object == null) {
				continue;
			}

			/*
			 * Expired keys are not written into
			 * the rewritten AOF.
			 */
			if (object.isExpired()) {
				continue;
			}

			byte[] value = object.copyValue();

			snapshot.add(RedisCommand.ofMixed("SET", key, value));

			/*
			 * Expiry is stored as an absolute Unix time in
			 * milliseconds, so a key keeps its real deadline
			 * no matter when the AOF is replayed.
			 */
			if (object.hasExpiration()) {
				snapshot.add(RedisCommand.of("PEXPIREAT", key, String.valueOf(object.getExpiresAt())));
			}
		}

		return snapshot;
	}

	/**
	 * Performs the actual background rewrite.
	 */
	private void performRewrite(List<RedisCommand> snapshot) {
		Path target = Path.of(properties.getAofFile());

		Path temp = Path.of(properties.getAofFile() + ".rewrite");

		try {
			Files.deleteIfExists(temp);

			/*
			 * Write the keyspace snapshot without
			 * blocking normal Redis mutations.
			 */
			writeCommands(temp, snapshot);

			/*
			 * We now need a very small critical section.
			 *
			 * No mutation can execute while we:
			 *
			 * 1. capture rewriteBuffer
			 * 2. append pending mutations
			 * 3. replace the AOF
			 */
			mutationLock.lock();

			try {
				List<RedisCommand> pending;

				synchronized (rewriteBufferLock) {
					pending = List.copyOf(rewriteBuffer);

					rewriteBuffer.clear();
				}

				/*
				 * Mutations that occurred after the
				 * snapshot must be replayed after
				 * the snapshot commands.
				 */
				appendCommandsToFile(temp, pending);

				/*
				 * Replace the old AOF with the
				 * rewritten AOF.
				 */
				moveAtomically(temp, target);

				rewriting.set(false);
			} finally {
				mutationLock.unlock();
			}
		} catch (Exception e) {
			/*
			 * Rewrite failed.
			 *
			 * The original AOF remains intact.
			 */
			rewriting.set(false);

			synchronized (rewriteBufferLock) {
				rewriteBuffer.clear();
			}

			try {
				Files.deleteIfExists(temp);
			} catch (IOException ignored) {
				// Best effort cleanup.
			}

			throw new IllegalStateException("AOF rewrite failed", e);
		}
	}

	/**
	 * Writes a complete AOF file from scratch.
	 */
	private void writeCommands(Path path, List<RedisCommand> commands) {
		try {
			Path parent = path.toAbsolutePath().getParent();

			if (parent != null) {
				Files.createDirectories(parent);
			}

			try (
				OutputStream output = Files.newOutputStream(
					path,
					StandardOpenOption.CREATE,
					StandardOpenOption.WRITE,
					StandardOpenOption.TRUNCATE_EXISTING
				)
			) {
				for (RedisCommand command : commands) {
					output.write(commandToResp(command));
				}

				output.flush();
			}
		} catch (IOException e) {
			throw new IllegalStateException("Unable to create AOF rewrite file: " + path, e);
		}
	}

	/**
	 * Appends pending commands to the rewrite file.
	 */
	private void appendCommandsToFile(Path path, List<RedisCommand> commands) {
		if (commands == null || commands.isEmpty()) {
			return;
		}

		try (
			OutputStream output = Files.newOutputStream(
				path,
				StandardOpenOption.CREATE,
				StandardOpenOption.WRITE,
				StandardOpenOption.APPEND
			)
		) {
			for (RedisCommand command : commands) {
				output.write(commandToResp(command));
			}

			output.flush();
		} catch (IOException e) {
			throw new IllegalStateException("Unable to append rewrite buffer", e);
		}
	}

	/**
	 * Replaces the target AOF with the rewritten file.
	 *
	 * Prefer an atomic move, but support file systems
	 * which do not provide ATOMIC_MOVE.
	 */
	private void moveAtomically(Path source, Path target) throws IOException {
		try {
			Files.move(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
		} catch (AtomicMoveNotSupportedException e) {
			Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
		}
	}

	/**
	 * Gracefully stops the rewrite executor.
	 */
	public void shutdown() {
		rewriteExecutor.shutdown();
	}
}
