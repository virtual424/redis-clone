package com.example.hbhagat.redis_clone.Storage.Eviction;

public final class LRUClock {

	private static final long CLOCK_MASK = 0x00FFFFFFL;

	private LRUClock() {}

	/**
	 * Redis-style 24-bit clock.
	 *
	 * Seconds are sufficient for approximate LRU.
	 */
	public static long currentClock() {
		return (System.currentTimeMillis() / 1000L) & CLOCK_MASK;
	}

	/**
	 * Calculates idle time while handling
	 * 24-bit clock wrap-around.
	 */
	public static long idleTime(long lastAccessedAt) {
		long current = currentClock();

		if (current >= lastAccessedAt) {
			return current - lastAccessedAt;
		}

		return (CLOCK_MASK - lastAccessedAt) + current;
	}
}
