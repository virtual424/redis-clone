package com.example.hbhagat.redis_clone.Protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public record RespBulkString(byte[] value) implements RespValue {
	@Override
	public byte[] encode() {
		if (value == null) {
			return "$-1\r\n".getBytes(StandardCharsets.UTF_8);
		}

		byte[] prefix = ("$" + value.length + "\r\n").getBytes(StandardCharsets.UTF_8);

		byte[] suffix = "\r\n".getBytes(StandardCharsets.UTF_8);

		byte[] result = new byte[prefix.length + value.length + suffix.length];

		System.arraycopy(prefix, 0, result, 0, prefix.length);
		System.arraycopy(value, 0, result, prefix.length, value.length);
		System.arraycopy(suffix, 0, result, prefix.length + value.length, suffix.length);

		return result;
	}

	public String utf8() {
		if (value == null) return null;
		return new String(value, StandardCharsets.UTF_8);
	}

	@Override
	public String toString() {
		if (value == null) return "null";

		return utf8();
	}

	public byte[] copy() {
		return Arrays.copyOf(value, value.length);
	}
}
