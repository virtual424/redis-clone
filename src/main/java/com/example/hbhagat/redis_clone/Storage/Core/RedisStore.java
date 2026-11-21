package com.example.hbhagat.redis_clone.Storage.Core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

@Component
public final class RedisStore {

	private final Map<String, RedisObject> store = new ConcurrentHashMap<>();

	public RedisObject newObject(byte[] value, long durationMs) {
		return RedisObjectFactory.create(value, durationMs);
	}

	public RedisObject newObject(byte[] value, long durationMs, byte type, byte encoding) {
		return RedisObjectFactory.create(value, durationMs, type, encoding);
	}

	public void put(String key, RedisObject object) {
		if (key == null) {
			throw new IllegalArgumentException("key cannot be null");
		}

		if (object == null) {
			throw new IllegalArgumentException("object cannot be null");
		}

		object.touch();

		store.put(key, object);
	}

	/**
	 * Raw lookup.
	 *
	 * Does NOT:
	 * - check expiration
	 * - remove expired key
	 * - update LRU
	 */
	public RedisObject peek(String key) {
		return store.get(key);
	}

	/**
	 * Normal read.
	 *
	 * Performs lazy expiration and updates LRU.
	 */
	public RedisObject get(String key) {
		RedisObject object = store.get(key);

		if (object == null) {
			return null;
		}

		if (object.isExpired()) {
			store.remove(key, object);

			return null;
		}

		object.touch();

		return object;
	}

	public boolean delete(String key) {
		return store.remove(key) != null;
	}

	/**
	 * Compare-and-delete.
	 *
	 * Useful for lazy expiration because another thread
	 * may have replaced the key after it was observed.
	 */
	public boolean deleteIfSame(String key, RedisObject expected) {
		if (key == null || expected == null) {
			return false;
		}

		return store.remove(key, expected);
	}

	public boolean contains(String key) {
		return store.containsKey(key);
	}

	public int size() {
		return store.size();
	}

	public Set<String> keys() {
		return Set.copyOf(store.keySet());
	}

	/**
	 * Snapshot specifically intended for
	 * eviction/expiration algorithms.
	 */
	public List<String> keysSnapshot() {
		return new ArrayList<>(store.keySet());
	}

	/**
	 * Snapshot for statistics and AOF rewrite.
	 */
	public List<Map.Entry<String, RedisObject>> entriesSnapshot() {
		return new ArrayList<>(store.entrySet());
	}
}
