package com.example.hbhagat.redis_clone.Storage.Eviction;

import com.example.hbhagat.redis_clone.Config.RedisProperties;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import org.springframework.stereotype.Component;

@Component
public final class EvictionManager {

	private final RedisProperties properties;

	private final Map<String, EvictionPolicy> policies;

	public EvictionManager(RedisProperties properties, List<EvictionPolicy> evictionPolicies) {
		this.properties = Objects.requireNonNull(properties, "RedisProperties cannot be null");

		Objects.requireNonNull(evictionPolicies, "Eviction policies cannot be null");

		this.policies =
			evictionPolicies
				.stream()
				.collect(
					Collectors.toUnmodifiableMap(
						EvictionPolicy::name,
						Function.identity(),
						(first, second) -> {
							throw new IllegalStateException("Duplicate eviction policy: " + first.name());
						}
					)
				);
	}

	/**
	 * Strategy Context.
	 *
	 * Selects the configured eviction strategy and
	 * delegates key selection to it.
	 *
	 * No deletion occurs here.
	 * No AOF knowledge exists here.
	 */
	public List<String> selectKeys(RedisStore store) {
		String strategy = properties.getEvictionStrategy();

		if (strategy == null || strategy.isBlank()) {
			throw new IllegalStateException("Redis eviction strategy is not configured");
		}

		EvictionPolicy policy = policies.get(strategy);

		if (policy == null) {
			throw new IllegalStateException("Unknown eviction strategy: " + strategy);
		}

		return policy.selectKeys(store, properties.getKeysLimit(), properties.getEvictionRatio());
	}
}
