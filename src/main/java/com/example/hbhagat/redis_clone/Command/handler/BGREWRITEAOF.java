package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespConstants;
import com.example.hbhagat.redis_clone.Storage.Persistence.AppendOnlyFileManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public final class BGREWRITEAOF implements CommandHandler {

	private final AppendOnlyFileManager aofManager;

	@Override
	public String command() {
		return "BGREWRITEAOF";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		/*
		 * BGREWRITEAOF does not mutate the Redis keyspace.
		 *
		 * It only requests an asynchronous rewrite of the
		 * current AOF.
		 */
		if (command.size() != 0) {
			return new com.example.hbhagat.redis_clone.Protocol.RespError(
				"ERR wrong number of arguments for 'bgrewriteaof' command"
			)
				.encode();
		}

		/*
		 * AOFManager owns the rewrite lifecycle.
		 *
		 * The command handler does not:
		 * - acquire mutation locks
		 * - inspect the store
		 * - build AOF commands
		 * - append anything to the AOF
		 */
		aofManager.rewrite();

		return RespConstants.OK;
	}
}
