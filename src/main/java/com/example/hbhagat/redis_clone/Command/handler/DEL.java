package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespError;
import com.example.hbhagat.redis_clone.Protocol.RespInteger;
import com.example.hbhagat.redis_clone.Storage.Mutation.Mutation;
import com.example.hbhagat.redis_clone.Storage.Mutation.MutationCoordinator;
import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public final class DEL implements CommandHandler {

	private final RedisStore store;

	private final MutationCoordinator mutationCoordinator;

	@Override
	public String command() {
		return "DEL";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		if (command.size() == 0) {
			return new RespError("ERR wrong number of arguments for 'del' command").encode();
		}

		int[] deletedCount = { 0 };

		mutationCoordinator.execute(() -> {
			for (String key : command.argumentsUtf8()) {
				if (store.delete(key)) {
					deletedCount[0]++;
				}
			}

			if (deletedCount[0] == 0) {
				return Mutation.empty();
			}

			/*
			 * The original DEL command accurately
			 * represents the logical mutation.
			 */
			return new Mutation(java.util.List.of(command));
		});

		return new RespInteger(deletedCount[0]).encode();
	}
}
