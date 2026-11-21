package com.example.hbhagat.redis_clone.support;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal blocking RESP client for tests.
 *
 * Replies map to: simple and bulk strings -> String (nil -> null),
 * integers -> Long, arrays -> List, errors -> RespClient.Error.
 */
public final class RespClient implements AutoCloseable {

	public record Error(String message) {}

	private final Socket socket;

	private final OutputStream out;

	private final InputStream in;

	public RespClient(int port) throws IOException {
		this.socket = new Socket("127.0.0.1", port);
		this.socket.setSoTimeout(5_000);
		this.out = socket.getOutputStream();
		this.in = new BufferedInputStream(socket.getInputStream());
	}

	public static byte[] encode(String... parts) {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		buffer.writeBytes(("*" + parts.length + "\r\n").getBytes(StandardCharsets.UTF_8));

		for (String part : parts) {
			byte[] bytes = part.getBytes(StandardCharsets.UTF_8);

			buffer.writeBytes(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
			buffer.writeBytes(bytes);
			buffer.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
		}

		return buffer.toByteArray();
	}

	public Object call(String... parts) throws IOException {
		sendRaw(encode(parts));

		return readReply();
	}

	public void sendRaw(byte[] bytes) throws IOException {
		out.write(bytes);
		out.flush();
	}

	public Object readReply() throws IOException {
		int type = in.read();

		if (type == -1) {
			throw new IOException("Connection closed by server");
		}

		String line = readLine();

		return switch (type) {
			case '+' -> line;
			case '-' -> new Error(line);
			case ':' -> Long.parseLong(line);
			case '$' -> readBulk(Integer.parseInt(line));
			case '*' -> {
				int count = Integer.parseInt(line);
				List<Object> items = new ArrayList<>();
				for (int i = 0; i < count; i++) {
					items.add(readReply());
				}
				yield items;
			}
			default -> throw new IOException("Unexpected reply type: " + (char) type);
		};
	}

	/**
	 * True if the server closed the connection.
	 */
	public boolean isClosedByServer() {
		try {
			return in.read() == -1;
		} catch (IOException e) {
			return true;
		}
	}

	private String readBulk(int length) throws IOException {
		if (length == -1) {
			return null;
		}

		byte[] bytes = in.readNBytes(length);

		in.readNBytes(2);

		return new String(bytes, StandardCharsets.UTF_8);
	}

	private String readLine() throws IOException {
		ByteArrayOutputStream line = new ByteArrayOutputStream();

		int b;

		while ((b = in.read()) != -1) {
			if (b == '\r') {
				in.read();
				break;
			}

			line.write(b);
		}

		return line.toString(StandardCharsets.UTF_8);
	}

	@Override
	public void close() throws IOException {
		socket.close();
	}
}
