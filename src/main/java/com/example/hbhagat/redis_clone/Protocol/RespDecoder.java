package com.example.hbhagat.redis_clone.Protocol;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class RespDecoder {

	private RespDecoder() {}

	public static List<RespArray> decode(byte[] data) throws IOException {
		Cursor cursor = new Cursor(data);

		List<RespArray> commands = new ArrayList<>();

		while (true) {
			skipCRLF(cursor);

			if (!cursor.hasRemaining()) {
				break;
			}

			RespValue value = decodeValue(cursor);

			if (!(value instanceof RespArray array)) {
				throw new IOException("Expected RESP Array");
			}

			commands.add(array);
		}

		return commands;
	}

	private static RespValue decodeValue(Cursor cursor) throws IOException {
		if (!cursor.hasRemaining()) {
			throw new IOException("Unexpected End Of Stream");
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

		if (cursor.remaining() < length + 2) {
			throw new IOException("Incomplete Bulk String");
		}

		byte[] bytes = Arrays.copyOfRange(cursor.data(), cursor.position(), cursor.position() + length);

		cursor.skip(length);

		expectCRLF(cursor);

		return new RespBulkString(bytes);
	}

	private static RespArray readArray(Cursor cursor) throws IOException {
		int count = readLength(cursor);

		List<RespValue> values = new ArrayList<>(count);

		for (int i = 0; i < count; i++) {
			values.add(decodeValue(cursor));
		}

		return new RespArray(values);
	}

	private static int readLength(Cursor cursor) throws IOException {
		byte[] bytes = readLine(cursor);

		return Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));
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

		throw new IOException("Unexpected End Of Stream");
	}

	private static void expectCRLF(Cursor cursor) throws IOException {
		if (cursor.remaining() < 2) {
			throw new IOException("Missing CRLF");
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

		private int position;

		Cursor(byte[] data) {
			this.data = data;
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
			return data.length - position;
		}

		boolean hasRemaining() {
			return remaining() > 0;
		}

		byte[] data() {
			return data;
		}
	}
}
