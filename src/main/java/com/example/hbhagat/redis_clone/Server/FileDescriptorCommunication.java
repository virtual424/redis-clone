package com.example.hbhagat.redis_clone.Server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public record FileDescriptorCommunication(SocketChannel channel) {
	public int read(byte[] data) throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(data);
		return channel.read(buffer);
	}

	public void write(byte[] data) throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(data);

		while (buffer.hasRemaining()) {
			channel.write(buffer);
		}
	}
}
