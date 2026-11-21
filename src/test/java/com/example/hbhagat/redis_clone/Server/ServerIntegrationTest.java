package com.example.hbhagat.redis_clone.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.hbhagat.redis_clone.support.RespClient;
import com.example.hbhagat.redis_clone.support.TestRedis;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end tests over a real TCP connection.
 */
class ServerIntegrationTest {

	@TempDir
	Path dir;

	@Test
	void basicCommands() throws Exception {
		try (TestRedis redis = new TestRedis(dir.resolve("a.aof")); RespClient client = redis.client()) {
			assertEquals("PONG", client.call("PING"));
			assertEquals("OK", client.call("SET", "k", "v"));
			assertEquals("v", client.call("GET", "k"));
			assertNull(client.call("GET", "missing"));
			assertEquals(1L, client.call("INCR", "n"));
			assertEquals(-1L, client.call("TTL", "k"));
			assertEquals(-2L, client.call("TTL", "missing"));
			assertEquals(1L, client.call("DEL", "k"));
			assertInstanceOf(RespClient.Error.class, client.call("NOPE"));
		}
	}

	@Test
	void largeValueRoundTrips() throws Exception {
		try (TestRedis redis = new TestRedis(dir.resolve("b.aof")); RespClient client = redis.client()) {
			String big = "x".repeat(1_000_000);

			assertEquals("OK", client.call("SET", "big", big));
			assertEquals(big, client.call("GET", "big"));
		}
	}

	@Test
	void pipelinedCommandsAllGetReplies() throws Exception {
		try (TestRedis redis = new TestRedis(dir.resolve("c.aof")); RespClient client = redis.client()) {
			ByteArrayOutputStream batch = new ByteArrayOutputStream();

			for (int i = 0; i < 10_000; i++) {
				batch.writeBytes(RespClient.encode("INCR", "counter"));
			}

			client.sendRaw(batch.toByteArray());

			Object last = null;

			for (int i = 0; i < 10_000; i++) {
				last = client.readReply();
			}

			assertEquals(10_000L, last);
		}
	}

	@Test
	void multiExecRunsQueuedCommandsTogether() throws Exception {
		try (TestRedis redis = new TestRedis(dir.resolve("d.aof")); RespClient client = redis.client()) {
			assertEquals("OK", client.call("MULTI"));
			assertEquals("QUEUED", client.call("SET", "a", "1"));
			assertEquals("QUEUED", client.call("INCR", "a"));
			assertEquals("QUEUED", client.call("GET", "a"));

			assertEquals(List.of("OK", 2L, "2"), client.call("EXEC"));
		}
	}

	@Test
	void evictionKeepsKeyCountAtTheLimit() throws Exception {
		try (
			TestRedis redis = new TestRedis(dir.resolve("e.aof"), p -> p.setKeysLimit(10));
			RespClient client = redis.client()
		) {
			for (int i = 0; i < 100; i++) {
				client.call("SET", "k" + i, "v");
			}

			assertTrue(redis.store.size() <= 10, "size " + redis.store.size());
			assertEquals("v", client.call("GET", "k99"), "the newest key is never the one evicted");
		}
	}

	@Test
	void connectionsAboveMaxClientsAreRefused() throws Exception {
		try (TestRedis redis = new TestRedis(dir.resolve("f.aof"), p -> p.setMaxClients(2))) {
			try (RespClient first = redis.client(); RespClient second = redis.client()) {
				assertEquals("PONG", first.call("PING"));
				assertEquals("PONG", second.call("PING"));

				try (RespClient third = redis.client()) {
					Object reply = third.readReply();

					assertInstanceOf(RespClient.Error.class, reply);
					assertTrue(((RespClient.Error) reply).message().contains("max number of clients"));
				}
			}
		}
	}
}
