package com.example.hbhagat.redis_clone.Storage.Eviction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import org.springframework.stereotype.Component;

@Component
public final class AllKeysRandomEvictionPolicy implements EvictionPolicy {

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

		Collections.shuffle(candidates);

		int count = Math.min(evictionCount, candidates.size());

		return new ArrayList<>(candidates.subList(0, count));
	}

	private int calculateEvictionCount(int keysLimit, double evictionRatio) {
		if (keysLimit <= 0) {
			return 1;
		}

		if (evictionRatio <= 0) {
			return 1;
		}

		long calculated = (long) Math.floor(keysLimit * evictionRatio);

		if (calculated <= 0) {
			return 1;
		}

		return (int) Math.min(Integer.MAX_VALUE, calculated);
	}

	@Override
	public String name() {
		return "allkeys-random";
	}
}
