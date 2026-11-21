package com.example.hbhagat.redis_clone.Storage.Eviction;

import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;

import java.util.List;

public interface EvictionPolicy {
	/**
	 * Select keys that should be evicted.
	 *
	 * IMPORTANT:
	 *
	 * The policy does NOT delete keys.
	 * The policy does NOT know about AOF.
	 * The policy does NOT know about Mutation.
	 *
	 * It only answers:
	 *
	 * "Which keys should be evicted?"
	 */
	List<String> selectKeys(RedisStore store, int keysLimit, double evictionRatio);

	/**
	 * Configuration name used by EvictionManager.
	 */
	String name();
}
