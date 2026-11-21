package com.example.hbhagat.redis_clone.Protocol;

import java.nio.charset.StandardCharsets;

public record RespError(String message) implements RespValue {
	@Override
	public byte[] encode() {
		return ("-" + message + "\r\n").getBytes(StandardCharsets.UTF_8);
	}
}
