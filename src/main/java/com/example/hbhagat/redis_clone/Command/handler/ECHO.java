package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespError;
import org.springframework.stereotype.Component;

@Component
public final class ECHO implements CommandHandler {

	@Override
	public String command() {
		return "ECHO";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		if (command.size() != 1) {
			return new RespError("ERR wrong number of arguments for 'echo' command").encode();
		}

		/*
		 * Return the original bulk string.
		 *
		 * Do not convert it to String and back to bytes.
		 * ECHO should preserve arbitrary byte content.
		 */
		return command.bulk(0).encode();
	}
}
