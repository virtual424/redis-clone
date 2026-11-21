package com.example.hbhagat.redis_clone.Storage.Persistence;

import com.example.hbhagat.redis_clone.Command.CommandDispatcher;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Config.RedisProperties;
import com.example.hbhagat.redis_clone.Protocol.CommandReader;
import com.example.hbhagat.redis_clone.Protocol.RespArray;
import com.example.hbhagat.redis_clone.Protocol.RespDecoder;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Rebuilds the keyspace at startup by replaying the AOF.
 *
 * Each logged command goes through the normal command
 * handlers, so replay uses exactly the same code path as a
 * live client. While loading, AppendOnlyFileManager does not
 * append, otherwise every restart would double the file.
 */
@Component
@RequiredArgsConstructor
public final class AofLoader {

	private static final Logger log = LoggerFactory.getLogger(AofLoader.class);

	private static final byte ERROR_REPLY = '-';

	private final RedisProperties properties;

	private final AppendOnlyFileManager aofManager;

	private final CommandDispatcher dispatcher;

	public record LoadResult(int commands, int failedCommands, int truncatedBytes) {}

	/**
	 * Replays the AOF if it exists.
	 *
	 * A command cut off at the end of the file (for example
	 * after a crash during a write) is dropped and the file is
	 * truncated to the last complete command, like Redis with
	 * aof-load-truncated yes. Corruption in the middle of the
	 * file stops startup with an error.
	 */
	public LoadResult load() throws IOException {
		Path path = Path.of(properties.getAofFile());

		if (!Files.exists(path)) {
			log.info("No AOF file at {}, starting with an empty keyspace", path.toAbsolutePath());

			return new LoadResult(0, 0, 0);
		}

		byte[] data = Files.readAllBytes(path);

		RespDecoder.DecodeResult decoded;

		try {
			decoded = RespDecoder.decodeAvailable(data, data.length);
		} catch (IOException e) {
			throw new IOException("AOF file is corrupted: " + path.toAbsolutePath() + " (" + e.getMessage() + ")", e);
		}

		int replayed = 0;

		int failed = 0;

		aofManager.beginLoading();

		try {
			for (RespArray array : decoded.commands()) {
				RedisCommand command = CommandReader.toCommand(array);

				byte[] reply = dispatcher.dispatch(command);

				replayed++;

				if (reply != null && reply.length > 0 && reply[0] == ERROR_REPLY) {
					failed++;

					log.warn("AOF command {} failed during replay", command.name());
				}
			}
		} finally {
			aofManager.endLoading();
		}

		int truncated = data.length - decoded.consumed();

		if (truncated > 0) {
			log.warn("AOF ends with an incomplete command ({} bytes); truncating it", truncated);

			try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
				channel.truncate(decoded.consumed());
			}
		}

		log.info("Loaded {} commands from {}", replayed, path.toAbsolutePath());

		return new LoadResult(replayed, failed, truncated);
	}
}
