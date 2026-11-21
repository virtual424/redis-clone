package com.example.hbhagat.redis_clone.support;

import com.example.hbhagat.redis_clone.Command.CommandDispatcher;
import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.handler.BGREWRITEAOF;
import com.example.hbhagat.redis_clone.Command.handler.CLIENT;
import com.example.hbhagat.redis_clone.Command.handler.DEL;
import com.example.hbhagat.redis_clone.Command.handler.ECHO;
import com.example.hbhagat.redis_clone.Command.handler.EXPIRE;
import com.example.hbhagat.redis_clone.Command.handler.GET;
import com.example.hbhagat.redis_clone.Command.handler.INCR;
import com.example.hbhagat.redis_clone.Command.handler.INFO;
import com.example.hbhagat.redis_clone.Command.handler.LATENCY;
import com.example.hbhagat.redis_clone.Command.handler.PEXPIREAT;
import com.example.hbhagat.redis_clone.Command.handler.PING;
import com.example.hbhagat.redis_clone.Command.handler.SET;
import com.example.hbhagat.redis_clone.Command.handler.SLEEP;
import com.example.hbhagat.redis_clone.Command.handler.TTL;
import com.example.hbhagat.redis_clone.Command.handler.UNKNOWN;
import com.example.hbhagat.redis_clone.Config.RedisProperties;
import com.example.hbhagat.redis_clone.Server.AsyncRedisServer;
import com.example.hbhagat.redis_clone.Server.ServerLifecycle;
import com.example.hbhagat.redis_clone.Storage.Core.RedisStore;
import com.example.hbhagat.redis_clone.Storage.Eviction.AllKeysLruEvictionPolicy;
import com.example.hbhagat.redis_clone.Storage.Eviction.AllKeysRandomEvictionPolicy;
import com.example.hbhagat.redis_clone.Storage.Eviction.EvictionManager;
import com.example.hbhagat.redis_clone.Storage.Eviction.SimpleFirstEvictionPolicy;
import com.example.hbhagat.redis_clone.Storage.Expiration.ExpirationManager;
import com.example.hbhagat.redis_clone.Storage.Mutation.MutationCoordinator;
import com.example.hbhagat.redis_clone.Storage.Persistence.AofLoader;
import com.example.hbhagat.redis_clone.Storage.Persistence.AppendOnlyFileManager;
import com.example.hbhagat.redis_clone.Storage.Statistics.RedisKeyspaceStatistics;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

/**
 * Wires the server by hand, the same way Spring does, so tests
 * can start and restart a real server on a free port in
 * milliseconds without an application context.
 */
public final class TestRedis implements AutoCloseable {

	public final RedisProperties properties;

	public final RedisStore store;

	public final AppendOnlyFileManager aof;

	public final MutationCoordinator mutations;

	public final ExpirationManager expiration;

	public final AsyncRedisServer server;

	/**
	 * Starts a server on a free port. The customizer can change
	 * any property before startup.
	 */
	public TestRedis(Path aofFile, Consumer<RedisProperties> customizer) throws Exception {
		properties = new RedisProperties();
		properties.setHost("127.0.0.1");
		properties.setPort(0);
		properties.setKeysLimit(10_000);
		properties.setEvictionStrategy("allkeys-lru");
		properties.setAofFile(aofFile.toString());

		customizer.accept(properties);

		store = new RedisStore();
		aof = new AppendOnlyFileManager(properties, store);
		mutations = new MutationCoordinator(aof);
		expiration = new ExpirationManager(store, mutations);

		EvictionManager eviction = new EvictionManager(
			properties,
			List.of(new SimpleFirstEvictionPolicy(), new AllKeysRandomEvictionPolicy(), new AllKeysLruEvictionPolicy())
		);

		List<CommandHandler> handlers = List.of(
			new BGREWRITEAOF(aof),
			new CLIENT(),
			new DEL(store, mutations),
			new ECHO(),
			new EXPIRE(store, mutations),
			new GET(store, mutations),
			new INCR(store, mutations),
			new INFO(new RedisKeyspaceStatistics(store)),
			new LATENCY(),
			new PEXPIREAT(store, mutations),
			new PING(),
			new SET(store, properties, eviction, mutations),
			new SLEEP(),
			new TTL(store, mutations),
			new UNKNOWN()
		);

		CommandDispatcher dispatcher = new CommandDispatcher(handlers);

		server = new AsyncRedisServer(properties, dispatcher, new ServerLifecycle(), new AofLoader(properties, aof, dispatcher), mutations);

		server.run();

		long deadline = System.currentTimeMillis() + 5_000;

		while (server.port() <= 0) {
			if (System.currentTimeMillis() > deadline) {
				throw new IllegalStateException("server did not start");
			}

			Thread.sleep(5);
		}
	}

	public TestRedis(Path aofFile) throws Exception {
		this(aofFile, properties -> {});
	}

	public int port() {
		return server.port();
	}

	public RespClient client() throws Exception {
		return new RespClient(port());
	}

	@Override
	public void close() {
		server.stop();
		server.awaitTermination();
		aof.shutdown();
	}
}
