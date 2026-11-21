package com.example.hbhagat.redis_clone.Storage.Eviction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.example.hbhagat.redis_clone.Storage.Core.RedisObject;
import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import org.springframework.stereotype.Component;

@Component
public final class AllKeysLruEvictionPolicy implements EvictionPolicy {

	private static final int SAMPLE_SIZE = 5;

	@Override
	public List<String> selectKeys(RedisStore store, int keysLimit, double evictionRatio) {
		if (store == null || store.size() == 0) {
			return List.of();
		}

		int evictionCount = calculateEvictionCount(keysLimit, evictionRatio);

		List<String> candidates = store.keysSnapshot();

		if (candidates.isEmpty()) {
			return List.of();
		}

		/*
		 * Randomize candidate selection first.
		 *
		 * This gives us approximate LRU sampling
		 * rather than repeatedly inspecting the same
		 * map iteration order.
		 */
		Collections.shuffle(candidates);

		int sampleSize = Math.min(SAMPLE_SIZE, candidates.size());

		List<String> sample = new ArrayList<>(candidates.subList(0, sampleSize));

		/*
		 * Highest idle time = least recently used.
		 */
		sample.sort(
			Comparator
				.comparingLong((String key) -> {
					RedisObject object = store.peek(key);

					if (object == null) {
						return Long.MIN_VALUE;
					}

					return LRUClock.idleTime(object.getLastAccessedAt());
				})
				.reversed()
		);

		int count = Math.min(evictionCount, sample.size());

		return new ArrayList<>(sample.subList(0, count));
	}

	private int calculateEvictionCount(int keysLimit, double evictionRatio) {
		if (keysLimit <= 0 || evictionRatio <= 0) {
			return 1;
		}

		long count = (long) Math.floor(keysLimit * evictionRatio);

		return (int) Math.min(Integer.MAX_VALUE, Math.max(1, count));
	}

	@Override
	public String name() {
		return "allkeys-lru";
	}
}
