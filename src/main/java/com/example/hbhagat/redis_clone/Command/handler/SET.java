package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Config.RedisProperties;
import com.example.hbhagat.redis_clone.Protocol.RespBulkString;
import com.example.hbhagat.redis_clone.Protocol.RespConstants;
import com.example.hbhagat.redis_clone.Protocol.RespError;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.example.hbhagat.redis_clone.Storage.Core.RedisObject;
import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import com.example.hbhagat.redis_clone.Storage.Eviction.EvictionManager;
import com.example.hbhagat.redis_clone.Storage.Mutation.Mutation;
import com.example.hbhagat.redis_clone.Storage.Mutation.MutationCoordinator;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public final class SET implements CommandHandler {

	private static final long INVALID_SYNTAX = Long.MIN_VALUE;

	private static final long INVALID_EXPIRATION = Long.MIN_VALUE + 1;

	private final RedisStore store;

	private final RedisProperties properties;

	private final EvictionManager evictionManager;

	private final MutationCoordinator mutationCoordinator;

	@Override
	public String command() {
		return "SET";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		List<RespBulkString> args = command.arguments();

		if (args.size() < 2) {
			return new RespError("ERR wrong number of arguments for 'SET' command").encode();
		}

		String key = args.get(0).utf8();

		byte[] value = args.get(1).value();

		long expirationMs = parseExpiration(args);

		if (expirationMs == INVALID_SYNTAX) {
			return new RespError("ERR syntax error").encode();
		}

		if (expirationMs == INVALID_EXPIRATION) {
			return new RespError("ERR invalid expire time in 'set' command").encode();
		}

		mutationCoordinator.execute(() -> {
			List<RedisCommand> mutations = new ArrayList<>();

			/*
			 * SET of an existing key does not
			 * consume another key slot.
			 *
			 * Expired existing keys should be
			 * treated as absent.
			 */
			RedisObject existing = store.peek(key);

			if (existing != null && existing.isExpired()) {
				store.deleteIfSame(key, existing);

				existing = null;
			}

			boolean newKey = existing == null;

			int keysLimit = properties.getKeysLimit();

			/*
			 * Eviction is required only when a
			 * genuinely new key would exceed capacity.
			 */
			if (newKey && keysLimit > 0 && store.size() >= keysLimit) {
				List<String> selectedKeys = evictionManager.selectKeys(store);

				/*
				 * The policy only selected keys.
				 *
				 * The mutation layer performs the
				 * actual deletion.
				 */
				for (String evictedKey : selectedKeys) {
					if (evictedKey == null) {
						continue;
					}

					RedisObject evictedObject = store.peek(evictedKey);

					if (evictedObject == null) {
						continue;
					}

					if (store.deleteIfSame(evictedKey, evictedObject)) {
						/*
						 * This DEL MUST appear before
						 * SET in the mutation.
						 */
						mutations.add(RedisCommand.of("DEL", evictedKey));
					}
				}
			}

			/*
			 * SET itself.
			 */
			RedisObject object = store.newObject(value, expirationMs);

			store.put(key, object);

			/*
			 * Log the plain SET, then the deadline as an
			 * absolute time. Logging "EX 10" would restart
			 * the 10 seconds every time the AOF is replayed.
			 */
			mutations.add(RedisCommand.ofBytes("SET", args.get(0).value(), value));

			if (object.hasExpiration()) {
				mutations.add(RedisCommand.of("PEXPIREAT", key, String.valueOf(object.getExpiresAt())));
			}

			return new Mutation(mutations);
		});

		return RespConstants.OK;
	}

	private long parseExpiration(List<RespBulkString> args) {
		long expirationMs = -1;

		for (int i = 2; i < args.size(); i++) {
			String option = args.get(i).utf8().toUpperCase(Locale.ROOT);

			if (!option.equals("EX")) {
				return INVALID_SYNTAX;
			}

			if (++i >= args.size()) {
				return INVALID_SYNTAX;
			}

			long seconds;

			try {
				seconds = Long.parseLong(args.get(i).utf8());
			} catch (NumberFormatException e) {
				return INVALID_EXPIRATION;
			}

			/*
			 * Redis SET EX requires a positive
			 * expiration.
			 */
			if (seconds <= 0) {
				return INVALID_EXPIRATION;
			}

			try {
				expirationMs = Math.multiplyExact(seconds, 1000L);
			} catch (ArithmeticException e) {
				return INVALID_EXPIRATION;
			}
		}

		return expirationMs;
	}
}
