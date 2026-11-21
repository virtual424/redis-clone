package com.example.hbhagat.redis_clone.Command;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

@Component
public final class CommandDispatcher {

	private final Map<String, CommandHandler> handlers;

	public CommandDispatcher(List<CommandHandler> commandHandlers) {
		this.handlers =
			commandHandlers
				.stream()
				.collect(Collectors.toUnmodifiableMap(handler -> handler.command().toUpperCase(), Function.identity()));
	}

	public byte[] dispatch(RedisCommand command) {
		if (command == null) {
			return handlers.get("UNKNOWN").execute(command);
		}

		String commandName = command.name().toUpperCase();

		CommandHandler handler = handlers.get(commandName);

		if (handler == null) {
			handler = handlers.get("UNKNOWN");
		}

		return handler.execute(command);
	}
}
