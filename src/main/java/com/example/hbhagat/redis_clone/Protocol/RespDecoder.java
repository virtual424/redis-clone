package com.example.hbhagat.redis_clone.Protocol;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class RespDecoder {

	private RespDecoder() {}

	/**
	 * Result of decoding as many complete commands as the
	 * buffer currently holds.
	 *
	 * consumed = number of bytes that belong to the complete
	 * commands. Bytes after it are the start of a command
	 * whose remaining bytes have not arrived yet.
	 */
	public record DecodeResult(List<RespArray> commands, int consumed) {}

	/**
	 * Thrown when the buffer ends in the middle of a frame.
	 *
	 * This is not a protocol error: TCP is a byte stream, so a
	 * command can arrive split across several reads. The caller
	 * keeps the bytes and tries again after the next read.
	 */
	static final class IncompleteFrameException extends IOException {

		IncompleteFrameException(String message) {
			super(message);
		}
	}

	/**
	 * Decodes every complete command in data[0, length).
	 *
	 * A partial command at the end is left unconsumed instead
	 * of being treated as an error. Malformed input still
	 * throws IOException.
	 */
	public static DecodeResult decodeAvailable(byte[] data, int length) throws IOException {
		Cursor cursor = new Cursor(data, length);

		List<RespArray> commands = new ArrayList<>();

		int consumed = 0;

		while (true) {
			skipCRLF(cursor);

			if (!cursor.hasRemaining()) {
				consumed = cursor.position();

				break;
			}

			int frameStart = cursor.position();

			try {
				RespValue value = decodeValue(cursor);

				if (!(value instanceof RespArray array)) {
					throw new IOException("Expected RESP Array");
				}

				commands.add(array);

				consumed = cursor.position();
			} catch (IncompleteFrameException e) {
				/*
				 * Wait for the rest of this frame.
				 */
				consumed = frameStart;

				break;
			}
		}

		return new DecodeResult(commands, consumed);
	}

	/**
	 * Decodes a buffer that must contain only complete commands.
	 */
	public static List<RespArray> decode(byte[] data) throws IOException {
		DecodeResult result = decodeAvailable(data, data.length);

		if (result.consumed() != data.length) {
			throw new IOException("Incomplete RESP frame");
		}

		return result.commands();
	}

	private static RespValue decodeValue(Cursor cursor) throws IOException {
		if (!cursor.hasRemaining()) {
			throw new IncompleteFrameException("Unexpected End Of Stream");
		}

		byte type = cursor.read();

		return switch (type) {
			case '+' -> readSimpleString(cursor);
			case '-' -> readError(cursor);
			case ':' -> readInteger(cursor);
			case '$' -> readBulkString(cursor);
			case '*' -> readArray(cursor);
			default -> throw new IOException("Unknown RESP Type : " + (char) type);
		};
	}

	private static RespSimpleString readSimpleString(Cursor cursor) throws IOException {
		byte[] bytes = readLine(cursor);

		return new RespSimpleString(new String(bytes, StandardCharsets.UTF_8));
	}

	private static RespError readError(Cursor cursor) throws IOException {
		byte[] bytes = readLine(cursor);

		return new RespError(new String(bytes, StandardCharsets.UTF_8));
	}

	private static RespInteger readInteger(Cursor cursor) throws IOException {
		byte[] bytes = readLine(cursor);

		return new RespInteger(Long.parseLong(new String(bytes, StandardCharsets.UTF_8)));
	}

	private static RespBulkString readBulkString(Cursor cursor) throws IOException {
		int length = readLength(cursor);

		if (length == -1) {
			return new RespBulkString(null);
		}

		if (length < -1) {
			throw new IOException("Invalid Bulk String length: " + length);
		}

		if (cursor.remaining() < (long) length + 2) {
			throw new IncompleteFrameException("Incomplete Bulk String");
		}

		byte[] bytes = Arrays.copyOfRange(cursor.data(), cursor.position(), cursor.position() + length);

		cursor.skip(length);

		expectCRLF(cursor);

		return new RespBulkString(bytes);
	}

	private static RespArray readArray(Cursor cursor) throws IOException {
		int count = readLength(cursor);

		if (count < -1) {
			throw new IOException("Invalid Array length: " + count);
		}

		/*
		 * Do not trust the declared count for pre-sizing:
		 * a client could announce a huge array.
		 */
		List<RespValue> values = new ArrayList<>(Math.max(0, Math.min(count, 1024)));

		for (int i = 0; i < count; i++) {
			values.add(decodeValue(cursor));
		}

		return new RespArray(values);
	}

	private static int readLength(Cursor cursor) throws IOException {
		byte[] bytes = readLine(cursor);

		try {
			return Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));
		} catch (NumberFormatException e) {
			throw new IOException("Invalid length: " + new String(bytes, StandardCharsets.UTF_8));
		}
	}

	private static byte[] readLine(Cursor cursor) throws IOException {
		int start = cursor.position();

		while (cursor.hasRemaining()) {
			if (cursor.current() == '\r') {
				int end = cursor.position();

				expectCRLF(cursor);

				return Arrays.copyOfRange(cursor.data(), start, end);
			}

			cursor.skip();
		}

		throw new IncompleteFrameException("Unexpected End Of Stream");
	}

	private static void expectCRLF(Cursor cursor) throws IOException {
		if (cursor.remaining() < 2) {
			throw new IncompleteFrameException("Missing CRLF");
		}

		if (cursor.read() != '\r') {
			throw new IOException("Expected CR");
		}

		if (cursor.read() != '\n') {
			throw new IOException("Expected LF");
		}
	}

	private static void skipCRLF(Cursor cursor) {
		while (cursor.hasRemaining()) {
			byte b = cursor.current();

			if (b == '\r' || b == '\n') {
				cursor.skip();
			} else {
				return;
			}
		}
	}

	private static final class Cursor {

		private final byte[] data;

		/*
		 * Bytes at index >= limit are not part of the input.
		 */
		private final int limit;

		private int position;

		Cursor(byte[] data, int limit) {
			this.data = data;
			this.limit = limit;
		}

		byte read() {
			return data[position++];
		}

		byte current() {
			return data[position];
		}

		void skip() {
			position++;
		}

		void skip(int n) {
			position += n;
		}

		int position() {
			return position;
		}

		int remaining() {
			return limit - position;
		}

		boolean hasRemaining() {
			return remaining() > 0;
		}

		byte[] data() {
			return data;
		}
	}
}
