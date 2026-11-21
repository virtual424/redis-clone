package com.example.hbhagat.redis_clone.Protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.hbhagat.redis_clone.Command.RedisCommand;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class CommandReaderTest {

	private static byte[] resp(String... parts) {
		StringBuilder sb = new StringBuilder("*" + parts.length + "\r\n");

		for (String part : parts) {
			sb.append('$').append(part.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(part).append("\r\n");
		}

		return sb.toString().getBytes(StandardCharsets.UTF_8);
	}

	@Test
	void commandSplitAtEveryPossibleByteIsDecodedOnce() throws IOException {
		byte[] frame = resp("SET", "key", "hello world");

		for (int split = 1; split < frame.length; split++) {
			CommandReader reader = new CommandReader();

			List<RedisCommand> first = reader.accept(Arrays.copyOfRange(frame, 0, split), split);

			assertTrue(first.isEmpty(), "no command before all bytes arrive (split " + split + ")");

			byte[] rest = Arrays.copyOfRange(frame, split, frame.length);

			List<RedisCommand> second = reader.accept(rest, rest.length);

			assertEquals(1, second.size(), "split at " + split);
			assertEquals("SET", second.get(0).name());
			assertEquals("hello world", second.get(0).argument(1));
			assertEquals(0, reader.pendingBytes());
		}
	}

	@Test
	void pipelinedCommandsInOneReadAreAllReturned() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		for (int i = 0; i < 1000; i++) {
			out.writeBytes(resp("INCR", "counter"));
		}

		byte[] data = out.toByteArray();

		List<RedisCommand> commands = new CommandReader().accept(data, data.length);

		assertEquals(1000, commands.size());
	}

	@Test
	void valueLargerThanReadChunkIsReassembled() throws IOException {
		String big = "x".repeat(200_000);

		byte[] frame = resp("SET", "big", big);

		CommandReader reader = new CommandReader();

		List<RedisCommand> result = new ArrayList<>();

		for (int offset = 0; offset < frame.length; offset += 16 * 1024) {
			int end = Math.min(frame.length, offset + 16 * 1024);

			result.addAll(reader.accept(Arrays.copyOfRange(frame, offset, end), end - offset));
		}

		assertEquals(1, result.size());
		assertEquals(200_000, result.get(0).bulk(1).value().length);
	}

	@Test
	void completeAndPartialCommandInSameRead() throws IOException {
		byte[] first = resp("PING");
		byte[] second = resp("ECHO", "hi");

		byte[] data = new byte[first.length + 5];

		System.arraycopy(first, 0, data, 0, first.length);
		System.arraycopy(second, 0, data, first.length, 5);

		CommandReader reader = new CommandReader();

		assertEquals(1, reader.accept(data, data.length).size());
		assertEquals(5, reader.pendingBytes());

		byte[] rest = Arrays.copyOfRange(second, 5, second.length);

		List<RedisCommand> commands = reader.accept(rest, rest.length);

		assertEquals(1, commands.size());
		assertEquals("hi", commands.get(0).argument(0));
	}

	@Test
	void malformedInputIsStillRejected() {
		byte[] data = "*1\r\n:42\r\n".getBytes(StandardCharsets.UTF_8);

		assertThrows(IOException.class, () -> new CommandReader().accept(data, data.length));

		byte[] badType = "?oops\r\n".getBytes(StandardCharsets.UTF_8);

		assertThrows(IOException.class, () -> new CommandReader().accept(badType, badType.length));
	}
}
