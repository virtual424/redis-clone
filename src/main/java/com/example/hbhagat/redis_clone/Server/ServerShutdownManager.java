package com.example.hbhagat.redis_clone.Server;

import com.example.hbhagat.redis_clone.Storage.Persistence.AppendOnlyFileManager;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public final class ServerShutdownManager {

	private static final Logger log = LoggerFactory.getLogger(ServerShutdownManager.class);

	private final ServerLifecycle lifecycle;

	private final AsyncRedisServer server;

	private final AppendOnlyFileManager aofManager;

	private final AtomicBoolean shutdownStarted = new AtomicBoolean(false);

	public ServerShutdownManager(ServerLifecycle lifecycle, AsyncRedisServer server, AppendOnlyFileManager aofManager) {
		this.lifecycle = lifecycle;
		this.server = server;
		this.aofManager = aofManager;
	}

	/**
	 * Gracefully shuts down the Redis server.
	 *
	 * Shutdown order:
	 *
	 * 1. Prevent new server work.
	 * 2. Wait for the current command batch to finish.
	 * 3. Wake/stop the selector.
	 * 4. Close client connections.
	 * 5. Wait for event-loop termination.
	 * 6. Stop AOF background executor.
	 *
	 * Spring itself is responsible for closing the
	 * ApplicationContext.
	 */
	public void shutdown() {
		if (!shutdownStarted.compareAndSet(false, true)) {
			return;
		}

		log.info("Redis server shutdown initiated");

		try {
			/*
			 * stop() performs:
			 *
			 * - graceful lifecycle transition
			 * - wait for current work
			 * - selector wakeup
			 * - server socket close
			 */
			server.stop();

			/*
			 * Wait for the event-loop thread to finish.
			 */
			server.awaitTermination();

			/*
			 * Stop AOF rewrite executor.
			 */
			aofManager.shutdown();

			log.info("Redis server shutdown completed");
		} catch (Exception e) {
			log.error("Error during Redis server shutdown", e);

			/*
			 * Make sure the lifecycle cannot remain in
			 * an intermediate state.
			 */
			if (!lifecycle.isShuttingDown()) {
				lifecycle.requestShutdownAndWait();
			}
		}
	}
}
