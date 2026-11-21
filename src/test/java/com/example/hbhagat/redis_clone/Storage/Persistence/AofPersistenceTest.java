package com.example.hbhagat.redis_clone.Storage.Persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.hbhagat.redis_clone.support.RespClient;
import com.example.hbhagat.redis_clone.support.TestRedis;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AofPersistenceTest {

	@TempDir
	Path dir;

	private Path aof() {
		return dir.resolve("appendonly.aof");
	}

	@Test
	void dataSurvivesRestart() throws Exception {
		try (TestRedis redis = new TestRedis(aof()); RespClient client = redis.client()) {
			client.call("SET", "name", "hardik");
			client.call("INCR", "visits");
			client.call("INCR", "visits");
			client.call("SET", "temp", "x");
			client.call("DEL", "temp");
		}

		try (TestRedis redis = new TestRedis(aof()); RespClient client = redis.client()) {
			assertEquals("hardik", client.call("GET", "name"));
			assertEquals("2", client.call("GET", "visits"));
			assertNull(client.call("GET", "temp"));
		}
	}

	@Test
	void replayDoesNotAppendToTheFileAgain() throws Exception {
		try (TestRedis redis = new TestRedis(aof()); RespClient client = redis.client()) {
			client.call("SET", "a", "1");
		}

		long sizeAfterFirstRun = Files.size(aof());

		new TestRedis(aof()).close();
		new TestRedis(aof()).close();

		assertEquals(sizeAfterFirstRun, Files.size(aof()));
	}

	@Test
	void ttlKeepsItsOriginalDeadlineAcrossRestart() throws Exception {
		try (TestRedis redis = new TestRedis(aof()); RespClient client = redis.client()) {
			client.call("SET", "session", "abc", "EX", "100");
			client.call("SET", "other", "y");
			client.call("EXPIRE", "other", "100");
		}

		String log = Files.readString(aof(), StandardCharsets.UTF_8);

		assertTrue(log.contains("PEXPIREAT"), "expiry is logged as an absolute time");
		assertFalse(log.contains("\r\nEX\r\n"), "relative EX is not logged");

		Thread.sleep(1_100);

		try (TestRedis redis = new TestRedis(aof()); RespClient client = redis.client()) {
			long ttl = (Long) client.call("TTL", "session");

			assertTrue(ttl >= 97 && ttl <= 99, "TTL kept counting down while the server was off, got " + ttl);
			assertEquals("abc", client.call("GET", "session"));
		}
	}

	@Test
	void keyThatExpiredWhileServerWasDownIsGone() throws Exception {
		try (TestRedis redis = new TestRedis(aof()); RespClient client = redis.client()) {
			client.call("SET", "short", "lived", "EX", "1");
			client.call("SET", "forever", "here");
		}

		Thread.sleep(1_200);

		try (TestRedis redis = new TestRedis(aof()); RespClient client = redis.client()) {
			assertNull(client.call("GET", "short"));
			assertEquals("here", client.call("GET", "forever"));
			assertEquals(1, redis.store.size());
		}
	}

	@Test
	void incompleteLastCommandIsDroppedAndFileTruncated() throws Exception {
		try (TestRedis redis = new TestRedis(aof()); RespClient client = redis.client()) {
			client.call("SET", "ok", "1");
		}

		long goodSize = Files.size(aof());

		/*
		 * Simulate a crash in the middle of writing a command.
		 */
		Files.write(aof(), "*3\r\n$3\r\nSET\r\n$4\r\nhal".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);

		try (TestRedis redis = new TestRedis(aof()); RespClient client = redis.client()) {
			assertEquals("1", client.call("GET", "ok"));
			assertEquals(goodSize, Files.size(aof()));

			client.call("SET", "after", "crash");
		}

		try (TestRedis redis = new TestRedis(aof()); RespClient client = redis.client()) {
			assertEquals("crash", client.call("GET", "after"));
		}
	}

	@Test
	void rewriteKeepsDataAndExpiry() throws Exception {
		try (TestRedis redis = new TestRedis(aof()); RespClient client = redis.client()) {
			for (int i = 0; i < 100; i++) {
				client.call("INCR", "counter");
			}

			client.call("SET", "session", "abc", "EX", "100");

			long before = Files.size(aof());

			assertEquals("OK", client.call("BGREWRITEAOF"));

			long deadline = System.currentTimeMillis() + 5_000;

			while (Files.size(aof()) >= before && System.currentTimeMillis() < deadline) {
				Thread.sleep(10);
			}

			assertTrue(Files.size(aof()) < before, "rewrite compacts 100 INCRs into one SET");
		}

		try (TestRedis redis = new TestRedis(aof()); RespClient client = redis.client()) {
			assertEquals("100", client.call("GET", "counter"));

			long ttl = (Long) client.call("TTL", "session");

			assertTrue(ttl > 90, "expiry survives the rewrite, got " + ttl);
		}
	}
}
