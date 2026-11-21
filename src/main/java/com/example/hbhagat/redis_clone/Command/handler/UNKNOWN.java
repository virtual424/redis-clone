package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespError;
import org.springframework.stereotype.Component;

@Component
public class UNKNOWN implements CommandHandler {
    @Override
    public String command() {
        return "UNKNOWN";
    }

    @Override
    public byte[] execute(RedisCommand command) {
        return new RespError("ERR unknown command '" + command.name() + "'").encode();
    }
}
