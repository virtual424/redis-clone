package com.example.hbhagat.redis_clone.Command;

public interface CommandHandler {
    String command();
    byte[] execute(RedisCommand command);
}
