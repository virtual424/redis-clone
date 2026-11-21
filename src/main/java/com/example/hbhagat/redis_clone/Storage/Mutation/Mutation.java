package com.example.hbhagat.redis_clone.Storage.Mutation;

import com.example.hbhagat.redis_clone.Command.RedisCommand;
import java.util.List;

public record Mutation(List<RedisCommand> commands) {
	public Mutation {
		commands = commands == null ? List.of() : List.copyOf(commands);
	}

	public static Mutation empty() {
		return new Mutation(List.of());
	}

	public boolean isEmpty() {
		return commands.isEmpty();
	}
}
