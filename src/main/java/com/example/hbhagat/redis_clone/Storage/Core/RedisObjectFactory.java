package com.example.hbhagat.redis_clone.Storage.Core;

import java.nio.charset.StandardCharsets;

public final class RedisObjectFactory {

	private static final int EMBSTR_MAX_LENGTH = 44;

	private RedisObjectFactory() {}

	public static RedisObject create(byte[] value, long durationMs) {
		long expiresAt = calculateExpiration(durationMs);

		byte encoding = deduceEncoding(value);

		return new RedisObject(value, RedisObject.TYPE_STRING, encoding, expiresAt);
	}

	public static RedisObject create(byte[] value, long durationMs, byte type, byte encoding) {
		long expiresAt = calculateExpiration(durationMs);

		return new RedisObject(value, type, encoding, expiresAt);
	}

	private static long calculateExpiration(long durationMs) {
		if (durationMs <= 0) {
			return -1;
		}

		try {
			return Math.addExact(System.currentTimeMillis(), durationMs);
		} catch (ArithmeticException e) {
			throw new IllegalArgumentException("Expiration duration is too large", e);
		}
	}

	private static byte deduceEncoding(byte[] value) {
		if (value == null) {
			return RedisObject.ENCODING_RAW;
		}

		String string = new String(value, StandardCharsets.UTF_8);

		try {
			Long.parseLong(string);

			return RedisObject.ENCODING_INT;
		} catch (NumberFormatException ignored) {
			// Not an integer.
		}

		if (value.length <= EMBSTR_MAX_LENGTH) {
			return RedisObject.ENCODING_EMBSTR;
		}

		return RedisObject.ENCODING_RAW;
	}
}
