package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespError;
import com.example.hbhagat.redis_clone.Protocol.RespSimpleString;
import org.springframework.stereotype.Component;

@Component
public final class PING implements CommandHandler {

	@Override
	public String command() {
		return "PING";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		if (command.size() > 1) {
			return new RespError("ERR wrong number of arguments for 'ping' command").encode();
		}

		/*
		 * PING
		 *
		 * -> +PONG
		 */
		if (command.size() == 0) {
			return new RespSimpleString("PONG").encode();
		}

		/*
		 * PING message
		 *
		 * -> returns the supplied bulk string.
		 *
		 * Use bulk() so arbitrary bytes are preserved.
		 */
		return command.bulk(0).encode();
	}
}
