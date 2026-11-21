package com.example.hbhagat.redis_clone.Protocol;

import com.example.hbhagat.redis_clone.Command.RedisCommand;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CommandReader {

	private final ByteBuffer buffer = ByteBuffer.allocate(8192);

	public List<RedisCommand> read(SocketChannel channel) throws IOException {
		int bytesRead = channel.read(buffer);

		if (bytesRead == -1) {
			return null;
		}

		if (bytesRead == 0) {
			return List.of();
		}

		byte[] data = Arrays.copyOf(buffer.array(), bytesRead);

		buffer.clear();

		List<RespArray> arrays = RespDecoder.decode(data);

		List<RedisCommand> commands = new ArrayList<>(arrays.size());

		for (RespArray array : arrays) {
			commands.add(toCommand(array));
		}

		return commands;
	}

	private RedisCommand toCommand(RespArray array) throws IOException {
		List<RespValue> values = array.values();

		if (values.isEmpty()) {
			throw new IOException("Empty RESP Array");
		}

		RespValue first = values.get(0);

		if (!(first instanceof RespBulkString commandName)) {
			throw new IOException("Command must be Bulk String");
		}

		String name = commandName.utf8().toUpperCase();

		List<RespBulkString> arguments = new ArrayList<>();

		for (int i = 1; i < values.size(); i++) {
			RespValue value = values.get(i);

			if (!(value instanceof RespBulkString bulk)) {
				throw new IOException("Arguments must be Bulk Strings");
			}

			arguments.add(bulk);
		}

		return new RedisCommand(name, arguments);
	}
}
