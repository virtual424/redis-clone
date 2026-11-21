package com.example.hbhagat.redis_clone.Storage.Core;

import java.util.Arrays;

import com.example.hbhagat.redis_clone.Storage.Eviction.LRUClock;
import lombok.Getter;

@Getter
public final class RedisObject {

	public static final byte TYPE_STRING = 0;

	public static final byte ENCODING_RAW = 0;
	public static final byte ENCODING_INT = 1;
	public static final byte ENCODING_EMBSTR = 8;

	private final byte[] value;

	private final byte type;

	private final byte encoding;

	/**
	 * -1 means no expiration.
	 */
	private long expiresAt;

	/**
	 * Approximate Redis-style LRU clock.
	 */
	private long lastAccessedAt;

	public RedisObject(byte[] value, byte type, byte encoding, long expiresAt) {
		this.value = value == null ? new byte[0] : Arrays.copyOf(value, value.length);

		this.type = type;

		this.encoding = encoding;

		this.expiresAt = expiresAt;

		this.lastAccessedAt = LRUClock.currentClock();
	}

	public boolean isString() {
		return type == TYPE_STRING;
	}

	public boolean isIntegerEncoded() {
		return encoding == ENCODING_INT;
	}

	public boolean hasExpiration() {
		return expiresAt != -1;
	}

	public boolean isExpired() {
		return hasExpiration() && expiresAt <= System.currentTimeMillis();
	}

	public void setExpiration(long expiresAt) {
		if (expiresAt < -1) {
			throw new IllegalArgumentException("expiresAt cannot be less than -1");
		}

		this.expiresAt = expiresAt;
	}

	public void removeExpiration() {
		this.expiresAt = -1;
	}

	public void touch() {
		this.lastAccessedAt = LRUClock.currentClock();
	}

	public byte[] copyValue() {
		return Arrays.copyOf(value, value.length);
	}
}
