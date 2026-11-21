package com.example.hbhagat.redis_clone.Storage.Core;

import java.nio.charset.StandardCharsets;

public final class RedisObjectEncoding {

	private RedisObjectEncoding() {}

	/**
	 * High 4 bits = type.
	 */
	public static byte getType(byte typeEncoding) {
		return (byte) (typeEncoding & 0xF0);
	}

	/**
	 * Low 4 bits = encoding.
	 */
	public static byte getEncoding(byte typeEncoding) {
		return (byte) (typeEncoding & 0x0F);
	}

	public static void assertType(byte typeEncoding, byte expectedType) {
		if (getType(typeEncoding) != expectedType) {
			throw new IllegalStateException("the operation is not permitted on this type");
		}
	}

	public static void assertEncoding(byte typeEncoding, byte expectedEncoding) {
		if (getEncoding(typeEncoding) != expectedEncoding) {
			throw new IllegalStateException("the operation is not permitted on this encoding");
		}
	}

	public static byte deduceTypeEncoding(byte[] value) {
		if (value == null) {
			return (byte) (RedisObject.TYPE_STRING | RedisObject.ENCODING_RAW);
		}

		String string = new String(value, StandardCharsets.UTF_8);

		try {
			Long.parseLong(string);

			return (byte) (RedisObject.TYPE_STRING | RedisObject.ENCODING_INT);
		} catch (NumberFormatException ignored) {}

		if (value.length <= 44) {
			return (byte) (RedisObject.TYPE_STRING | RedisObject.ENCODING_EMBSTR);
		}

		return (byte) (RedisObject.TYPE_STRING | RedisObject.ENCODING_RAW);
	}
}
