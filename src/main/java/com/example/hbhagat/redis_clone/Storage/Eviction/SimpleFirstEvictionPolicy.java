package com.example.hbhagat.redis_clone.Storage.Eviction;

import java.util.List;

import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import org.springframework.stereotype.Component;

@Component
public final class SimpleFirstEvictionPolicy implements EvictionPolicy {

	@Override
	public List<String> selectKeys(RedisStore store, int keysLimit, double evictionRatio) {
		if (store == null || store.size() == 0) {
			return List.of();
		}

		List<String> candidates = store.keysSnapshot();

		if (candidates.isEmpty()) {
			return List.of();
		}

		/*
		 * Simple-first means select the first
		 * available key.
		 *
		 * Actual deletion is performed by
		 * SET/mutation layer.
		 */
		return List.of(candidates.get(0));
	}

	@Override
	public String name() {
		return "simple-first";
	}
}
