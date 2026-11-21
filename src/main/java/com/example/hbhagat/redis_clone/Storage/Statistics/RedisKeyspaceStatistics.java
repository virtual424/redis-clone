package com.example.hbhagat.redis_clone.Storage.Statistics;

import com.example.hbhagat.redis_clone.Storage.Core.RedisObject;
import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import org.springframework.stereotype.Component;

@Component
public final class RedisKeyspaceStatistics implements KeyspaceStatistics {

	private final RedisStore store;

	public RedisKeyspaceStatistics(RedisStore store) {
		this.store = store;
	}

	@Override
	public long keyCount() {
		long count = 0;

		for (var entry : store.entriesSnapshot()) {
			RedisObject object = entry.getValue();

			if (!object.isExpired()) {
				count++;
			}
		}

		return count;
	}

	@Override
	public long expiringKeyCount() {
		long count = 0;

		for (var entry : store.entriesSnapshot()) {
			RedisObject object = entry.getValue();

			if (!object.isExpired() && object.hasExpiration()) {
				count++;
			}
		}

		return count;
	}

	@Override
	public long averageTtl() {
		long total = 0;

		long count = 0;

		long now = System.currentTimeMillis();

		for (var entry : store.entriesSnapshot()) {
			RedisObject object = entry.getValue();

			if (object.isExpired() || !object.hasExpiration()) {
				continue;
			}

			long ttl = object.getExpiresAt() - now;

			if (ttl <= 0) {
				continue;
			}

			total += ttl;

			count++;
		}

		return count == 0 ? 0 : total / count;
	}
}
