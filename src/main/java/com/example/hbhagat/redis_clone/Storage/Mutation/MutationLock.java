package com.example.hbhagat.redis_clone.Storage.Mutation;

import java.util.concurrent.locks.ReentrantLock;
import org.springframework.stereotype.Component;

@Component
public final class MutationLock {

	private final ReentrantLock lock = new ReentrantLock();

	public void lock() {
		lock.lock();
	}

	public void unlock() {
		lock.unlock();
	}

	public boolean isHeldByCurrentThread() {
		return lock.isHeldByCurrentThread();
	}
}
