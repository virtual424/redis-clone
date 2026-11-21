package com.example.hbhagat.redis_clone.Protocol;

import java.nio.charset.StandardCharsets;

public record RespInteger(long value) implements RespValue {
	@Override
	public byte[] encode() {
		return (":" + value + "\r\n").getBytes(StandardCharsets.UTF_8);
	}
}
