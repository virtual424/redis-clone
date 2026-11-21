package com.example.hbhagat.redis_clone.Server;

import com.example.hbhagat.redis_clone.Command.CommandDispatcher;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Config.RedisProperties;
import com.example.hbhagat.redis_clone.Storage.Persistence.AofLoader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.NonNull;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public final class AsyncRedisServer implements CommandLineRunner {

	private final RedisProperties redisProperties;

	private final CommandDispatcher commandDispatcher;

	private final ServerLifecycle lifecycle;

	private final AofLoader aofLoader;

	private final Map<SocketChannel, ClientConnection> clients = new ConcurrentHashMap<>();

	private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
		Thread thread = new Thread(runnable, "redis-event-loop");

		thread.setDaemon(true);

		return thread;
	});

	/*
	 * Accessed by the event-loop thread and shutdown thread.
	 */
	private volatile Selector selector;

	private volatile ServerSocketChannel serverChannel;

	private volatile boolean started = false;

	/*
	 * Port actually bound; differs from the configured port
	 * when port 0 (any free port) is used, e.g. in tests.
	 */
	private volatile int boundPort = -1;

	/**
	 * Starts the asynchronous Redis TCP server.
	 */
	public void start() throws IOException {
		synchronized (this) {
			if (started) {
				return;
			}

			started = true;
		}

		System.out.println(
			"Starting asynchronous TCP server on " + redisProperties.getHost() + ":" + redisProperties.getPort()
		);

		/*
		 * Rebuild the keyspace from the AOF before accepting
		 * clients, so nobody reads a half-loaded state.
		 */
		aofLoader.load();

		try (ServerSocketChannel server = ServerSocketChannel.open(); Selector openedSelector = Selector.open()) {
			this.serverChannel = server;
			this.selector = openedSelector;

			/*
			 * Bind TCP server.
			 */
			server.bind(new InetSocketAddress(redisProperties.getHost(), redisProperties.getPort()));

			boundPort = ((InetSocketAddress) server.getLocalAddress()).getPort();

			/*
			 * Non-blocking server socket.
			 */
			server.configureBlocking(false);

			/*
			 * Register server socket for ACCEPT.
			 */
			server.register(openedSelector, SelectionKey.OP_ACCEPT);

			System.out.println("Server started");

			eventLoop(server, openedSelector);
		} finally {
			/*
			 * Close all connected clients.
			 */
			closeAllClients();

			this.serverChannel = null;
			this.selector = null;
			this.boundPort = -1;

			synchronized (this) {
				started = false;
			}

			/*
			 * Make sure lifecycle is in the final state.
			 */
			lifecycle.requestShutdownAndWait();
		}
	}

	/**
	 * Main selector/event loop.
	 */
	private void eventLoop(ServerSocketChannel server, Selector selector) {
		while (!lifecycle.isShuttingDown()) {
			int ready;

			try {
				/*
				 * This blocks without consuming CPU.
				 *
				 * stop() calls selector.wakeup() when shutdown
				 * is requested.
				 */
				ready = selector.select();
			} catch (IOException e) {
				if (lifecycle.isShuttingDown()) {
					break;
				}

				System.err.println("Selector failure: " + e.getMessage());

				break;
			}

			/*
			 * Shutdown could have happened while select()
			 * was blocked.
			 */
			if (lifecycle.isShuttingDown()) {
				break;
			}

			/*
			 * selector.wakeup() may cause select() to return
			 * with zero events.
			 */
			if (ready == 0) {
				continue;
			}

			/*
			 * Never transition back into BUSY after shutdown
			 * has been requested.
			 */
			if (!lifecycle.tryEnterBusy()) {
				if (lifecycle.isShuttingDown()) {
					break;
				}

				continue;
			}

			try {
				processSelectedKeys(selector);
			} finally {
				/*
				 * If shutdown was requested during processing,
				 * finishBusy() transitions directly to
				 * SHUTTING_DOWN.
				 */
				lifecycle.finishBusy();
			}
		}
	}

	/**
	 * Processes one batch of selector events.
	 */
	private void processSelectedKeys(Selector selector) {
		Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

		while (iterator.hasNext()) {
			SelectionKey key = iterator.next();

			iterator.remove();

			if (!key.isValid()) {
				continue;
			}

			try {
				if (key.isAcceptable()) {
					/*
					 * Do not accept new clients after shutdown
					 * has been requested.
					 */
					if (!lifecycle.isShuttingDown()) {
						acceptClient((ServerSocketChannel) key.channel(), selector);
					}
				} else if (key.isReadable()) {
					readClient(key);
				}
			} catch (IOException e) {
				closeClientQuietly(key);
			} catch (Exception e) {
				System.err.println("Error processing client event: " + e.getMessage());

				closeClientQuietly(key);
			}
		}
	}

	/**
	 * Accept a new client connection.
	 */
	private void acceptClient(ServerSocketChannel server, Selector selector) throws IOException {
		SocketChannel client = server.accept();

		if (client == null) {
			return;
		}

		/*
		 * Race protection:
		 *
		 * shutdown could have started between select()
		 * and accept().
		 */
		if (lifecycle.isShuttingDown()) {
			client.close();
			return;
		}

		client.configureBlocking(false);

		ClientConnection connection = new ClientConnection(client);

		clients.put(client, connection);

		client.register(selector, SelectionKey.OP_READ);

		System.out.println("Client connected " + client.getRemoteAddress() + " clients=" + clients.size());
	}

	/**
	 * Reads and executes all complete commands currently
	 * available for a client.
	 */
	private void readClient(SelectionKey key) throws Exception {
		SocketChannel client = (SocketChannel) key.channel();

		ClientConnection connection = clients.get(client);

		if (connection == null) {
			return;
		}

		List<RedisCommand> commands = connection.reader().read(client);

		if (commands == null) {
			connection.resetTransaction();
			closeClient(key);
			return;
		}

		for (RedisCommand command : commands) {
			if (command == null) {
				continue;
			}

			System.out.println("Executing : " + command);

			byte[] response;

			String commandName = command.name().toUpperCase(java.util.Locale.ROOT);

			/*
			 * MULTI
			 */
			if ("MULTI".equals(commandName)) {
				response = connection.beginTransaction(command);
				/*
				 * EXEC
				 */
			} else if ("EXEC".equals(commandName)) {
				response = connection.executeTransaction(command, commandDispatcher);
				/*
				 * DISCARD
				 */
			} else if ("DISCARD".equals(commandName)) {
				response = connection.discardTransaction(command);
				/*
				 * Normal command while inside MULTI.
				 */
			} else if (connection.isTransactionActive()) {
				response = connection.queueCommand(command);
				/*
				 * Normal command outside MULTI.
				 */
			} else {
				response = commandDispatcher.dispatch(command);
			}

			if (response != null && response.length > 0) {
				connection.comm().write(response);
			}
		}
	}

	/**
	 * Close a single client.
	 */
	private void closeClient(SelectionKey key) throws IOException {
		SocketChannel channel = (SocketChannel) key.channel();

		clients.remove(channel);

		key.cancel();

		if (channel.isOpen()) {
			channel.close();
		}

		System.out.println("Client disconnected clients=" + clients.size());
	}

	/**
	 * Close a client without allowing cleanup failure
	 * to break shutdown.
	 */
	private void closeClientQuietly(SelectionKey key) {
		try {
			closeClient(key);
		} catch (IOException ignored) {
			/*
			 * Best effort cleanup.
			 */
		}
	}

	/**
	 * Close all currently connected clients.
	 */
	private void closeAllClients() {
		List<SocketChannel> snapshot = new ArrayList<>(clients.keySet());

		for (SocketChannel channel : snapshot) {
			clients.remove(channel);

			try {
				channel.close();
			} catch (IOException ignored) {
				/*
				 * Best effort cleanup.
				 */
			}
		}

		clients.clear();
	}

	/**
	 * The listening port, or -1 when the server is not running.
	 */
	public int port() {
		return boundPort;
	}

	/**
	 * Initiates server shutdown.
	 *
	 * Important:
	 *
	 * selector.select() can block indefinitely.
	 *
	 * selector.wakeup() is therefore required to wake the
	 * event loop immediately.
	 */
	public void stop() {
		lifecycle.requestShutdownAndWait();

		Selector currentSelector = this.selector;

		if (currentSelector != null) {
			currentSelector.wakeup();
		}

		ServerSocketChannel currentServer = this.serverChannel;

		if (currentServer != null) {
			try {
				currentServer.close();
			} catch (IOException ignored) {
				/*
				 * Best effort cleanup.
				 */
			}
		}
	}

	/**
	 * Waits for the event-loop thread to terminate.
	 */
	public void awaitTermination() {
		executor.shutdown();

		try {
			if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
				executor.shutdownNow();

				if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
					System.err.println("Redis event loop did not terminate cleanly");
				}
			}
		} catch (InterruptedException e) {
			executor.shutdownNow();

			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void run(String @NonNull... args) {
		executor.submit(() -> {
			try {
				start();
			} catch (IOException e) {
				if (!lifecycle.isShuttingDown()) {
					System.err.println("Redis server failed to start: " + e.getMessage());
				}
			}
		});
	}
}
