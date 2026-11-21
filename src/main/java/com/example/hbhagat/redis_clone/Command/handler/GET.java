package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespBulkString;
import com.example.hbhagat.redis_clone.Protocol.RespError;
import com.example.hbhagat.redis_clone.Storage.Mutation.Mutation;
import com.example.hbhagat.redis_clone.Storage.Mutation.MutationCoordinator;
import com.example.hbhagat.redis_clone.Storage.Core.RedisObject;
import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public final class GET implements CommandHandler {

	private final RedisStore store;

	private final MutationCoordinator mutationCoordinator;

	@Override
	public String command() {
		return "GET";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		if (command.size() != 1) {
			return new RespError("ERR wrong number of arguments for 'get' command").encode();
		}

		String key = command.argument(0);

		RedisObject object = store.peek(key);

		if (object == null) {
			return new RespBulkString(null).encode();
		}

		if (object.isExpired()) {
			RedisObject expected = object;

			mutationCoordinator.execute(() -> {
				RedisObject current = store.peek(key);

				if (current == null || current != expected || !current.isExpired()) {
					return Mutation.empty();
				}

				if (!store.deleteIfSame(key, expected)) {
					return Mutation.empty();
				}

				return new Mutation(java.util.List.of(RedisCommand.of("DEL", key)));
			});

			return new RespBulkString(null).encode();
		}

		if (!object.isString()) {
			return new RespError("WRONGTYPE Operation against a key holding the wrong kind of value").encode();
		}

		object.touch();

		return new RespBulkString(object.getValue()).encode();
	}
}
