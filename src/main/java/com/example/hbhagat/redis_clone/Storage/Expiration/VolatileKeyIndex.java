package com.example.hbhagat.redis_clone.Storage.Expiration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The set of keys that have a TTL, with O(1) random sampling.
 *
 * Redis keeps keys with a TTL in a separate "expires" dictionary
 * and samples it randomly. A HashSet cannot return a random
 * element without iterating, so this index stores keys in an
 * array (random pick by index) plus a map from key to its
 * position (O(1) removal by swapping with the last element).
 */
public final class VolatileKeyIndex {

	private final List<String> keys = new ArrayList<>();

	private final Map<String, Integer> positions = new HashMap<>();

	public synchronized void add(String key) {
		if (positions.containsKey(key)) {
			return;
		}

		positions.put(key, keys.size());

		keys.add(key);
	}

	public synchronized void remove(String key) {
		Integer position = positions.remove(key);

		if (position == null) {
			return;
		}

		int last = keys.size() - 1;

		String lastKey = keys.remove(last);

		if (position != last) {
			keys.set(position, lastKey);

			positions.put(lastKey, position);
		}
	}

	/**
	 * Up to count distinct keys chosen at random.
	 */
	public synchronized List<String> sample(int count) {
		int size = keys.size();

		if (size <= count) {
			return new ArrayList<>(keys);
		}

		/*
		 * Partial Fisher-Yates over a copy of the positions we
		 * touch, so the index itself is not reordered.
		 */
		ThreadLocalRandom random = ThreadLocalRandom.current();

		Map<Integer, Integer> swapped = new HashMap<>();

		List<String> result = new ArrayList<>(count);

		for (int i = 0; i < count; i++) {
			int j = i + random.nextInt(size - i);

			int valueAtJ = swapped.getOrDefault(j, j);

			swapped.put(j, swapped.getOrDefault(i, i));

			result.add(keys.get(valueAtJ));
		}

		return result;
	}

	public synchronized int size() {
		return keys.size();
	}
}
