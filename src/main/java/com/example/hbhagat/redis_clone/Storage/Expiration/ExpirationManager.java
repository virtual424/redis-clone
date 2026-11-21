package com.example.hbhagat.redis_clone.Storage.Expiration;

import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Storage.Core.RedisObject;
import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import com.example.hbhagat.redis_clone.Storage.Mutation.Mutation;
import com.example.hbhagat.redis_clone.Storage.Mutation.MutationCoordinator;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * Active expiry, modelled on Redis's activeExpireCycle.
 *
 * Lazy expiry (on read) never frees keys nobody reads again.
 * Checking every key would be O(n), so Redis samples instead:
 *
 * 1. Pick 20 random keys that have a TTL.
 * 2. Delete the expired ones.
 * 3. If more than 25% were expired, many more probably are:
 *    repeat, within a 25 ms time budget so clients are not
 *    blocked for long.
 *
 * Statistically this keeps expired-but-not-deleted keys below
 * about 25% of all keys with a TTL.
 */
@Component
public final class ExpirationManager {

	static final int SAMPLE_SIZE = 20;

	static final double REPEAT_THRESHOLD = 0.25;

	static final long TIME_BUDGET_NANOS = 25_000_000L;

	private final RedisStore store;

	private final MutationCoordinator mutationCoordinator;

	public ExpirationManager(RedisStore store, MutationCoordinator mutationCoordinator) {
		this.store = store;
		this.mutationCoordinator = mutationCoordinator;
	}

	/**
	 * Runs one active expiry cycle.
	 *
	 * Returns the number of keys deleted.
	 */
	public int deleteExpiredKeys() {
		long start = System.nanoTime();

		int totalDeleted = 0;

		while (true) {
			int[] sampled = { 0 };

			int[] expired = { 0 };

			mutationCoordinator.execute(() -> {
				List<String> sample = store.sampleVolatileKeys(SAMPLE_SIZE);

				sampled[0] = sample.size();

				List<RedisCommand> deletions = new ArrayList<>();

				for (String key : sample) {
					RedisObject object = store.peek(key);

					if (object == null || !object.hasExpiration()) {
						store.forgetVolatile(key);

						continue;
					}

					if (object.isExpired() && store.deleteIfSame(key, object)) {
						deletions.add(RedisCommand.of("DEL", key));

						expired[0]++;
					}
				}

				return new Mutation(deletions);
			});

			totalDeleted += expired[0];

			boolean fewExpired = expired[0] <= sampled[0] * REPEAT_THRESHOLD;

			boolean outOfTime = System.nanoTime() - start > TIME_BUDGET_NANOS;

			if (sampled[0] == 0 || fewExpired || outOfTime) {
				return totalDeleted;
			}
		}
	}
}
