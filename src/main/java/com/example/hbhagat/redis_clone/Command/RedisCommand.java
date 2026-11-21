package com.example.hbhagat.redis_clone.Command;

import com.example.hbhagat.redis_clone.Protocol.RespBulkString;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class RedisCommand {

	private final String name;

	private final List<RespBulkString> arguments;

	public RedisCommand(String name, List<RespBulkString> arguments) {
		if (name == null || name.isBlank()) {
			throw new IllegalArgumentException("Command name cannot be null or blank");
		}

		if (arguments == null) {
			throw new IllegalArgumentException("Command arguments cannot be null");
		}

		this.name = name.toUpperCase();

		this.arguments = List.copyOf(arguments);
	}

	/**
	 * Creates a command from RESP bulk-string arguments.
	 */
	public static RedisCommand of(String name, List<RespBulkString> arguments) {
		return new RedisCommand(name, arguments);
	}

	/**
	 * Creates a command using String arguments.
	 *
	 * Useful for internally generated commands such as:
	 *
	 * SET key value EX 10
	 * DEL key
	 */
	public static RedisCommand of(String name, String... arguments) {
		List<RespBulkString> bulkStrings = new ArrayList<>(arguments.length);

		for (String argument : arguments) {
			if (argument == null) {
				throw new IllegalArgumentException("Command argument cannot be null");
			}

			bulkStrings.add(new RespBulkString(argument.getBytes(StandardCharsets.UTF_8)));
		}

		return new RedisCommand(name, bulkStrings);
	}

	/**
	 * Creates a command using raw byte arguments.
	 *
	 * This is important for AOF rewrite because
	 * Redis values are binary-safe.
	 */
	public static RedisCommand ofBytes(String name, byte[]... arguments) {
		List<RespBulkString> bulkStrings = new ArrayList<>(arguments.length);

		for (byte[] argument : arguments) {
			if (argument == null) {
				throw new IllegalArgumentException("Command argument cannot be null");
			}

			bulkStrings.add(new RespBulkString(argument));
		}

		return new RedisCommand(name, bulkStrings);
	}

	public String name() {
		return name;
	}

	public List<RespBulkString> arguments() {
		return arguments;
	}

	public int size() {
		return arguments.size();
	}

	public RespBulkString bulk(int index) {
		return arguments.get(index);
	}

	public String argument(int index) {
		return bulk(index).utf8();
	}

	public List<String> argumentsUtf8() {
		List<String> result = new ArrayList<>(arguments.size());

		for (RespBulkString argument : arguments) {
			result.add(argument.utf8());
		}

		return result;
	}

	@Override
	public String toString() {
		return name + " " + String.join(" ", argumentsUtf8());
	}

	public static RedisCommand ofMixed(String name, Object... arguments) {
		List<RespBulkString> bulkStrings = new ArrayList<>(arguments.length);

		for (Object argument : arguments) {
			if (argument instanceof String string) {
				bulkStrings.add(new RespBulkString(string.getBytes(StandardCharsets.UTF_8)));
			} else if (argument instanceof byte[] bytes) {
				bulkStrings.add(new RespBulkString(bytes));
			} else {
				throw new IllegalArgumentException(
					"Unsupported command argument type: " + argument.getClass().getName()
				);
			}
		}

		return new RedisCommand(name, bulkStrings);
	}
}
