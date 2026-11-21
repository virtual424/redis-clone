package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespArray;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public final class LATENCY implements CommandHandler {

	@Override
	public String command() {
		return "LATENCY";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		return new RespArray(List.of()).encode();
	}
}
