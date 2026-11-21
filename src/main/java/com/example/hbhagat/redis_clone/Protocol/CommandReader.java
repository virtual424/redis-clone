package com.example.hbhagat.redis_clone.Protocol;

import com.example.hbhagat.redis_clone.Command.RedisCommand;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Turns the byte stream of one client connection into commands.
 *
 * TCP delivers bytes, not messages. One read can contain
 * half a command, exactly one command, or many pipelined
 * commands. This reader keeps unfinished bytes in a per-client
 * query buffer and only returns commands that are complete.
 */
public class CommandReader {

	private static final int READ_CHUNK_SIZE = 16 * 1024;

	/*
	 * Protects the server from a client that keeps sending
	 * an unfinished command. Real Redis uses
	 * client-query-buffer-limit (1 GB by default).
	 */
	private static final int MAX_QUERY_BUFFER_SIZE = 64 * 1024 * 1024;

	private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_CHUNK_SIZE);

	/*
	 * Query buffer: bytes received but not yet decoded.
	 */
	private byte[] pending = new byte[READ_CHUNK_SIZE];

	private int pendingLength = 0;

	/**
	 * Reads once from the channel.
	 *
	 * Returns null when the client closed the connection,
	 * and an empty list when no complete command is available yet.
	 */
	public List<RedisCommand> read(SocketChannel channel) throws IOException {
		readBuffer.clear();

		int bytesRead = channel.read(readBuffer);

		if (bytesRead == -1) {
			return null;
		}

		if (bytesRead == 0) {
			return List.of();
		}

		return accept(readBuffer.array(), bytesRead);
	}

	/**
	 * Adds received bytes to the query buffer and returns every
	 * command that is now complete.
	 *
	 * Separate from read() so it can be tested without a socket.
	 */
	public List<RedisCommand> accept(byte[] data, int length) throws IOException {
		append(data, length);

		RespDecoder.DecodeResult result = RespDecoder.decodeAvailable(pending, pendingLength);

		discardConsumed(result.consumed());

		List<RedisCommand> commands = new ArrayList<>(result.commands().size());

		for (RespArray array : result.commands()) {
			commands.add(toCommand(array));
		}

		return commands;
	}

	/**
	 * Bytes waiting for the rest of a command.
	 */
	public int pendingBytes() {
		return pendingLength;
	}

	private void append(byte[] data, int length) throws IOException {
		int required = pendingLength + length;

		if (required > MAX_QUERY_BUFFER_SIZE) {
			throw new IOException("Query buffer limit exceeded");
		}

		if (required > pending.length) {
			int newSize = Math.min(MAX_QUERY_BUFFER_SIZE, Math.max(required, pending.length * 2));

			pending = Arrays.copyOf(pending, newSize);
		}

		System.arraycopy(data, 0, pending, pendingLength, length);

		pendingLength = required;
	}

	private void discardConsumed(int consumed) {
		if (consumed == 0) {
			return;
		}

		int remaining = pendingLength - consumed;

		System.arraycopy(pending, consumed, pending, 0, remaining);

		pendingLength = remaining;

		/*
		 * Give memory back after a large value has been processed.
		 */
		if (pendingLength == 0 && pending.length > READ_CHUNK_SIZE * 4) {
			pending = new byte[READ_CHUNK_SIZE];
		}
	}

	private RedisCommand toCommand(RespArray array) throws IOException {
		List<RespValue> values = array.values();

		if (values.isEmpty()) {
			throw new IOException("Empty RESP Array");
		}

		RespValue first = values.get(0);

		if (!(first instanceof RespBulkString commandName) || commandName.value() == null) {
			throw new IOException("Command must be Bulk String");
		}

		String name = commandName.utf8().toUpperCase();

		List<RespBulkString> arguments = new ArrayList<>();

		for (int i = 1; i < values.size(); i++) {
			RespValue value = values.get(i);

			if (!(value instanceof RespBulkString bulk) || bulk.value() == null) {
				throw new IOException("Arguments must be Bulk Strings");
			}

			arguments.add(bulk);
		}

		return new RedisCommand(name, arguments);
	}
}
