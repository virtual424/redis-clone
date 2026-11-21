package com.example.hbhagat.redis_clone.Storage.Statistics;

public interface KeyspaceStatistics {
	long keyCount();

	long expiringKeyCount();

	long averageTtl();
}
