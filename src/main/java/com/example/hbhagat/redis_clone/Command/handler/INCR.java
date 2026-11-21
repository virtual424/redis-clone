package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespError;
import com.example.hbhagat.redis_clone.Protocol.RespInteger;
import com.example.hbhagat.redis_clone.Storage.Mutation.Mutation;
import com.example.hbhagat.redis_clone.Storage.Mutation.MutationCoordinator;
import com.example.hbhagat.redis_clone.Storage.Core.RedisObject;
import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public final class INCR implements CommandHandler {

	private final RedisStore store;

	private final MutationCoordinator mutationCoordinator;

	@Override
	public String command() {
		return "INCR";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		if (command.size() != 1) {
			return new RespError("ERR wrong number of arguments for 'incr' command").encode();
		}

		String key = command.argument(0);

		long[] result = { 0 };

		String[] error = { null };

		mutationCoordinator.execute(() -> {
			RedisObject object = store.peek(key);

			/*
			 * Missing key behaves as integer 0.
			 */
			if (object == null) {
				object =
					store.newObject(
						"0".getBytes(StandardCharsets.UTF_8),
						-1,
						RedisObject.TYPE_STRING,
						RedisObject.ENCODING_INT
					);

				store.put(key, object);
			}

			/*
			 * Lazy expiration.
			 *
			 * An expired key behaves as missing.
			 */
			if (object.isExpired()) {
				store.deleteIfSame(key, object);

				object =
					store.newObject(
						"0".getBytes(StandardCharsets.UTF_8),
						-1,
						RedisObject.TYPE_STRING,
						RedisObject.ENCODING_INT
					);

				store.put(key, object);
			}

			if (!object.isString()) {
				error[0] = "WRONGTYPE Operation against a key holding the wrong kind of value";

				return Mutation.empty();
			}

			if (!object.isIntegerEncoded()) {
				error[0] = "ERR value is not an integer or out of range";

				return Mutation.empty();
			}

			long current;

			try {
				current = Long.parseLong(new String(object.getValue(), StandardCharsets.UTF_8));
			} catch (NumberFormatException e) {
				error[0] = "ERR value is not an integer or out of range";

				return Mutation.empty();
			}

			if (current == Long.MAX_VALUE) {
				error[0] = "ERR increment or decrement would overflow";

				return Mutation.empty();
			}

			long incremented = current + 1;

			byte[] newValue = String.valueOf(incremented).getBytes(StandardCharsets.UTF_8);

			RedisObject newObject = store.newObject(newValue, -1, RedisObject.TYPE_STRING, RedisObject.ENCODING_INT);

			/*
			 * INCR preserves TTL.
			 */
			newObject.setExpiration(object.getExpiresAt());

			store.put(key, newObject);

			result[0] = incremented;

			/*
			 * Logical mutation.
			 */
			return new Mutation(List.of(command));
		});

		if (error[0] != null) {
			return new RespError(error[0]).encode();
		}

		return new RespInteger(result[0]).encode();
	}
}
