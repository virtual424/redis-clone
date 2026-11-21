package com.example.hbhagat.redis_clone.Storage.Mutation;

import java.util.function.Supplier;

import com.example.hbhagat.redis_clone.Storage.Persistence.AppendOnlyFileManager;
import org.springframework.stereotype.Component;

@Component
public final class MutationCoordinator {

	private final AppendOnlyFileManager aofManager;

	public MutationCoordinator(AppendOnlyFileManager aofManager) {
		this.aofManager = aofManager;
	}

	/**
	 * Executes a Redis state mutation and persists the resulting
	 * logical mutation through AOF.
	 *
	 * Command handlers interact only with MutationCoordinator.
	 * They must never call AOFManager directly.
	 */
	public MutationResult execute(Supplier<Mutation> mutationSupplier) {
		if (mutationSupplier == null) {
			return MutationResult.empty();
		}

		return aofManager.executeMutation(mutationSupplier);
	}
}
