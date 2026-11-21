package com.example.hbhagat.redis_clone.Storage.Expiration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.hbhagat.redis_clone.support.RespClient;
import com.example.hbhagat.redis_clone.support.TestRedis;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ActiveExpiryTest {

	@TempDir
	Path dir;

	@Test
	void expiredKeysThatNobodyReadsAreDeleted() throws Exception {
		try (TestRedis redis = new TestRedis(dir.resolve("a.aof")); RespClient client = redis.client()) {
			for (int i = 0; i < 200; i++) {
				client.call("SET", "temp:" + i, "v", "EX", "1");
			}

			for (int i = 0; i < 50; i++) {
				client.call("SET", "keep:" + i, "v");
			}

			assertEquals(250, redis.store.size());

			Thread.sleep(1_100);

			/*
			 * Every sampled key is expired, so one cycle keeps
			 * repeating until the expired keys are gone.
			 */
			int deleted = redis.expiration.deleteExpiredKeys();

			assertEquals(200, deleted);
			assertEquals(50, redis.store.size());
			assertEquals(0, redis.store.volatileKeyCount());
		}
	}

	@Test
	void cycleStopsEarlyWhenFewSampledKeysAreExpired() throws Exception {
		try (TestRedis redis = new TestRedis(dir.resolve("b.aof")); RespClient client = redis.client()) {
			for (int i = 0; i < 1_000; i++) {
				client.call("SET", "long:" + i, "v", "EX", "1000");
			}

			client.call("SET", "short", "v", "EX", "1");

			Thread.sleep(1_100);

			int deleted = redis.expiration.deleteExpiredKeys();

			assertTrue(deleted <= 1, "one cycle samples about 20 of 1001 keys and stops");
			assertTrue(redis.store.size() >= 1_000);
		}
	}

	@Test
	void ttlIndexFollowsTheKeyspace() throws Exception {
		try (TestRedis redis = new TestRedis(dir.resolve("c.aof")); RespClient client = redis.client()) {
			client.call("SET", "a", "1", "EX", "100");
			client.call("SET", "b", "1");
			client.call("EXPIRE", "b", "100");

			assertEquals(2, redis.store.volatileKeyCount());

			client.call("SET", "a", "2");

			assertEquals(1, redis.store.volatileKeyCount(), "plain SET clears the TTL");

			client.call("DEL", "b");

			assertEquals(0, redis.store.volatileKeyCount());
		}
	}

	@Test
	void sampleReturnsDistinctKeys() {
		VolatileKeyIndex index = new VolatileKeyIndex();

		for (int i = 0; i < 100; i++) {
			index.add("k" + i);
		}

		index.remove("k5");
		index.remove("k99");

		for (int round = 0; round < 50; round++) {
			List<String> sample = index.sample(20);

			Set<String> unique = new HashSet<>(sample);

			assertEquals(20, unique.size());
			assertTrue(!unique.contains("k5") && !unique.contains("k99"));
		}

		assertEquals(98, index.size());
		assertEquals(98, index.sample(500).size());
	}
}
