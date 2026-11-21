package com.example.hbhagat.redis_clone.Storage.Mutation;

public record MutationResult(Mutation mutation) {
	public static MutationResult empty() {
		return new MutationResult(Mutation.empty());
	}

	public boolean changed() {
		return mutation != null && !mutation.isEmpty();
	}
}
