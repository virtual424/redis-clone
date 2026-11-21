package com.example.hbhagat.redis_clone.Server;

import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.stereotype.Component;

@Component
public final class ServerLifecycle {

	private final Object monitor = new Object();

	private volatile ServerState state = ServerState.WAITING;

	/*
	 * Separate shutdown request from the actual state.
	 *
	 * This allows us to remember that shutdown was requested
	 * while the event loop is currently BUSY.
	 */
	private boolean shutdownRequested = false;

	public ServerState state() {
		return state;
	}

	public boolean isWaiting() {
		return state == ServerState.WAITING;
	}

	public boolean isBusy() {
		return state == ServerState.BUSY;
	}

	public boolean isShuttingDown() {
		return state == ServerState.SHUTTING_DOWN;
	}

	/**
	 * WAITING -> BUSY
	 *
	 * Shutdown must win over new work.
	 *
	 * Therefore this transition is allowed only when:
	 *
	 *     state == WAITING
	 *     shutdownRequested == false
	 */
	public boolean tryEnterBusy() {
		synchronized (monitor) {
			if (state != ServerState.WAITING) {
				return false;
			}

			if (shutdownRequested) {
				state = ServerState.SHUTTING_DOWN;
				monitor.notifyAll();
				return false;
			}

			state = ServerState.BUSY;
			return true;
		}
	}

	/**
	 * BUSY -> WAITING
	 *
	 * If shutdown was requested while BUSY, we transition directly
	 * to SHUTTING_DOWN instead of going back to WAITING.
	 *
	 * This is important:
	 *
	 *     BUSY
	 *       |
	 *       | shutdown requested
	 *       v
	 *     SHUTTING_DOWN
	 *
	 * We never allow:
	 *
	 *     SHUTTING_DOWN -> BUSY
	 */
	public void finishBusy() {
		synchronized (monitor) {
			if (state != ServerState.BUSY) {
				return;
			}

			if (shutdownRequested) {
				state = ServerState.SHUTTING_DOWN;
			} else {
				state = ServerState.WAITING;
			}

			monitor.notifyAll();
		}
	}

	/**
	 * Request shutdown and wait until the currently executing
	 * operation has completed.
	 *
	 * This method deliberately uses wait/notify instead of a
	 * busy-spin.
	 */
	public void requestShutdownAndWait() {
		synchronized (monitor) {
			shutdownRequested = true;

			/*
			 * If the event loop is not processing anything,
			 * shutdown can begin immediately.
			 */
			if (state == ServerState.WAITING) {
				state = ServerState.SHUTTING_DOWN;
				monitor.notifyAll();
				return;
			}

			/*
			 * If BUSY, wait for the event loop to finish the
			 * current operation.
			 */
			while (state == ServerState.BUSY) {
				try {
					monitor.wait();
				} catch (InterruptedException e) {
					/*
					 * Preserve interrupt status but continue
					 * waiting until the current mutation/request
					 * has safely completed.
					 */
					Thread.currentThread().interrupt();
				}
			}

			/*
			 * finishBusy() normally performs this transition.
			 * This is defensive.
			 */
			if (state != ServerState.SHUTTING_DOWN) {
				state = ServerState.SHUTTING_DOWN;
			}

			monitor.notifyAll();
		}
	}

	/**
	 * Used by the event loop after selector.select() returns.
	 *
	 * If shutdown has already started, the event loop must not
	 * begin processing another batch of commands.
	 */
	public boolean shouldStop() {
		synchronized (monitor) {
			return state == ServerState.SHUTTING_DOWN || shutdownRequested;
		}
	}
}
