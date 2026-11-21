package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespConstants;
import org.springframework.stereotype.Component;

@Component
public final class CLIENT implements CommandHandler {

	@Override
	public String command() {
		return "CLIENT";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		return RespConstants.OK;
	}
}
