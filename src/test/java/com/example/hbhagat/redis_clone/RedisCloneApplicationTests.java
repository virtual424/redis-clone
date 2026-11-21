package com.example.hbhagat.redis_clone;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/*
 * Port 0 picks a free port; the AOF goes to target/ so tests
 * never touch your real appendonly.aof.
 */
@SpringBootTest(properties = { "redis.port=0", "redis.aof-file=target/test-appendonly.aof" })
class RedisCloneApplicationTests {

	@Test
	void contextLoads() {
	}

}
