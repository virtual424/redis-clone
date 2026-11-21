package com.example.hbhagat.redis_clone.Protocol;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public record RespArray(List<RespValue> values) implements RespValue {
	@Override
	public byte[] encode() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		out.writeBytes(("*" + values.size() + "\r\n").getBytes(StandardCharsets.UTF_8));

		for (RespValue value : values) {
			out.writeBytes(value.encode());
		}

		return out.toByteArray();
	}
}
