package com.example.hbhagat.redis_clone.Storage.Expiration;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ExpirationSchedular {

	private final ExpirationManager expirationManager;

	/*
	 * Default 100 ms: 10 cycles per second, like Redis.
	 */
	@Scheduled(fixedDelayString = "${redis.active-expire-interval-ms:100}")
	public void run() {
		expirationManager.deleteExpiredKeys();
	}
}
