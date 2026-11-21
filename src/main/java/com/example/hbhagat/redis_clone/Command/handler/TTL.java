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
public final class TTL implements CommandHandler {

	private final RedisStore store;

	private final MutationCoordinator mutationCoordinator;

	@Override
	public String command() {
		return "TTL";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		if (command.size() != 1) {
			return new RespError("ERR wrong number of arguments for 'ttl' command").encode();
		}

		String key = command.argument(0);

		RedisObject object = store.peek(key);

		if (object == null) {
			return new RespInteger(-2).encode();
		}

		if (object.isExpired()) {
			RedisObject expected = object;

			mutationCoordinator.execute(() -> {
				RedisObject current = store.peek(key);

				if (current == null || current != expected || !current.isExpired()) {
					return Mutation.empty();
				}

				if (store.deleteIfSame(key, expected)) {
					return new Mutation(java.util.List.of(RedisCommand.of("DEL", key)));
				}

				return Mutation.empty();
			});

			return new RespInteger(-2).encode();
		}

		if (!object.hasExpiration()) {
			return new RespInteger(-1).encode();
		}

		long remainingMs = object.getExpiresAt() - System.currentTimeMillis();

		if (remainingMs <= 0) {
			return new RespInteger(-2).encode();
		}

		long remainingSeconds = remainingMs / 1000L;

		return new RespInteger(remainingSeconds).encode();
	}
}
