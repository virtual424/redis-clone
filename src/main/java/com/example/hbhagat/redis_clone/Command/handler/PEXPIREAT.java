package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespError;
import com.example.hbhagat.redis_clone.Protocol.RespInteger;
import com.example.hbhagat.redis_clone.Storage.Core.RedisObject;
import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import com.example.hbhagat.redis_clone.Storage.Mutation.Mutation;
import com.example.hbhagat.redis_clone.Storage.Mutation.MutationCoordinator;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * PEXPIREAT key unix-time-milliseconds
 *
 * Sets an absolute expiry time. SET ... EX and EXPIRE are
 * written to the AOF in this form, so a key expires at the
 * same moment whether or not the server restarted.
 *
 * Returns 1 if the timeout was set (or the key was deleted
 * because the time is already in the past), 0 if the key
 * does not exist.
 */
@Component
@RequiredArgsConstructor
public final class PEXPIREAT implements CommandHandler {

	private final RedisStore store;

	private final MutationCoordinator mutationCoordinator;

	@Override
	public String command() {
		return "PEXPIREAT";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		if (command.size() != 2) {
			return new RespError("ERR wrong number of arguments for 'pexpireat' command").encode();
		}

		String key = command.argument(0);

		long expiresAt;

		try {
			expiresAt = Long.parseLong(command.argument(1));
		} catch (NumberFormatException e) {
			return new RespError("ERR value is not an integer or out of range").encode();
		}

		long[] result = { 0 };

		mutationCoordinator.execute(() -> {
			RedisObject object = store.peek(key);

			if (object == null) {
				return Mutation.empty();
			}

			if (object.isExpired() || expiresAt <= System.currentTimeMillis()) {
				/*
				 * Already past the deadline: delete now,
				 * like Redis does.
				 */
				boolean wasLive = !object.isExpired();

				if (store.deleteIfSame(key, object)) {
					result[0] = wasLive ? 1 : 0;

					return new Mutation(List.of(RedisCommand.of("DEL", key)));
				}

				return Mutation.empty();
			}

			object.setExpiration(expiresAt);

			result[0] = 1;

			return new Mutation(List.of(command));
		});

		return new RespInteger(result[0]).encode();
	}
}
