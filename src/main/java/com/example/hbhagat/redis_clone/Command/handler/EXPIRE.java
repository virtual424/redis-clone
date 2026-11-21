package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespError;
import com.example.hbhagat.redis_clone.Protocol.RespInteger;
import com.example.hbhagat.redis_clone.Storage.Mutation.Mutation;
import com.example.hbhagat.redis_clone.Storage.Mutation.MutationCoordinator;
import com.example.hbhagat.redis_clone.Storage.Core.RedisObject;
import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public final class EXPIRE implements CommandHandler {

	private final RedisStore store;

	private final MutationCoordinator mutationCoordinator;

	@Override
	public String command() {
		return "EXPIRE";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		if (command.size() != 2) {
			return new RespError("ERR wrong number of arguments for 'expire' command").encode();
		}

		String key = command.argument(0);

		long seconds;

		try {
			seconds = Long.parseLong(command.argument(1));
		} catch (NumberFormatException e) {
			return new RespError("ERR value is not an integer or out of range").encode();
		}

		/*
		 * EXPIRE accepts non-negative seconds.
		 */
		if (seconds < 0) {
			return new RespInteger(0).encode();
		}

		final long[] result = { 0 };

		final long finalSeconds = seconds;

		mutationCoordinator.execute(() -> {
			RedisObject object = store.peek(key);

			if (object == null) {
				return Mutation.empty();
			}

			if (object.isExpired()) {
				store.deleteIfSame(key, object);

				return Mutation.empty();
			}

			long expiresAt;

			try {
				expiresAt = Math.addExact(System.currentTimeMillis(), Math.multiplyExact(finalSeconds, 1000L));
			} catch (ArithmeticException e) {
				return Mutation.empty();
			}

			object.setExpiration(expiresAt);

			result[0] = 1;

			/*
			 * Persist the absolute deadline, not the relative
			 * seconds, so replaying the AOF later does not
			 * extend the key's life.
			 */
			return new Mutation(java.util.List.of(RedisCommand.of("PEXPIREAT", key, String.valueOf(expiresAt))));
		});

		return new RespInteger(result[0]).encode();
	}
}
