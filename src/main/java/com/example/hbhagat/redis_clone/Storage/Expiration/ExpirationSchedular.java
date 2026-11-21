package com.example.hbhagat.redis_clone.Storage.Expiration;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ExpirationSchedular {

	private final ExpirationManager expirationManager;

	@Scheduled(fixedRate = 1000 * 60)
	public void run() {
		expirationManager.deleteExpiredKeys();
	}
}
