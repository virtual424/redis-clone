package com.example.hbhagat.redis_clone.Config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "redis")
public class RedisProperties {

	private String host = "localhost";

	private int port = 6379;

	private int maxClients = 100;

	private int keysLimit = 100;

	private double evictionRatio = 0.40;

	/*
	 * Supported:
	 *
	 * simple-first
	 * allkeys-random
	 * allkeys-lru
	 */
	private String evictionStrategy = "allkeys-random";

	/*
	 * Path of the append-only file. Relative paths resolve
	 * against the working directory.
	 */
	private String aofFile = "appendonly.aof";
}
