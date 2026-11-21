package com.example.hbhagat.redis_clone.Storage.Expiration;

import com.example.hbhagat.redis_clone.Command.RedisCommand;
import java.util.ArrayList;
import java.util.List;

import com.example.hbhagat.redis_clone.Storage.Core.RedisObject;
import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import com.example.hbhagat.redis_clone.Storage.Mutation.Mutation;
import com.example.hbhagat.redis_clone.Storage.Mutation.MutationCoordinator;
import org.springframework.stereotype.Component;

@Component
public final class ExpirationManager {

	private static final int SAMPLE_SIZE = 20;

	private final RedisStore store;

	private final MutationCoordinator mutationCoordinator;

	public ExpirationManager(RedisStore store, MutationCoordinator mutationCoordinator) {
		this.store = store;
		this.mutationCoordinator = mutationCoordinator;
	}

	public void deleteExpiredKeys() {
		if (store.size() == 0) {
			return;
		}

		mutationCoordinator.execute(() -> {
			List<RedisCommand> mutations = new ArrayList<>();

			List<String> keys = store.keysSnapshot();

			int inspected = 0;

			for (String key : keys) {
				if (inspected >= SAMPLE_SIZE) {
					break;
				}

				inspected++;

				RedisObject object = store.peek(key);

				if (object == null) {
					continue;
				}

				if (!object.isExpired()) {
					continue;
				}

				/*
				 * Compare-and-delete protects against
				 * a replacement of the same key.
				 */
				if (store.deleteIfSame(key, object)) {
					mutations.add(RedisCommand.of("DEL", key));
				}
			}

			return new Mutation(mutations);
		});
	}
}
