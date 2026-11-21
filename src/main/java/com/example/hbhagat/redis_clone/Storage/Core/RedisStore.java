package com.example.hbhagat.redis_clone.Storage.Core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import com.example.hbhagat.redis_clone.Storage.Expiration.VolatileKeyIndex;
import org.springframework.stereotype.Component;

@Component
public final class RedisStore {

	private final Map<String, RedisObject> store = new ConcurrentHashMap<>();

	/*
	 * Keys that have a TTL, kept separately (like Redis's
	 * "expires" dictionary) so active expiry can sample them.
	 */
	private final VolatileKeyIndex volatileKeys = new VolatileKeyIndex();

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

		if (object.hasExpiration()) {
			volatileKeys.add(key);
		} else {
			volatileKeys.remove(key);
		}
	}

	/**
	 * Sets an absolute expiry on an existing key and makes it
	 * visible to active expiry.
	 */
	public void setExpiration(String key, RedisObject object, long expiresAt) {
		object.setExpiration(expiresAt);

		volatileKeys.add(key);
	}

	/**
	 * Random keys that have a TTL, for active expiry.
	 */
	public List<String> sampleVolatileKeys(int count) {
		return volatileKeys.sample(count);
	}

	/**
	 * Drops a key from the TTL index when it no longer has a
	 * TTL (stale entry found while sampling).
	 */
	public void forgetVolatile(String key) {
		volatileKeys.remove(key);
	}

	public int volatileKeyCount() {
		return volatileKeys.size();
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
			if (store.remove(key, object)) {
				volatileKeys.remove(key);
			}

			return null;
		}

		object.touch();

		return object;
	}

	public boolean delete(String key) {
		boolean removed = store.remove(key) != null;

		if (removed) {
			volatileKeys.remove(key);
		}

		return removed;
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

		boolean removed = store.remove(key, expected);

		if (removed) {
			volatileKeys.remove(key);
		}

		return removed;
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
