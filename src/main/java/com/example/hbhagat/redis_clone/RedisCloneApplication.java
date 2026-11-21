package com.example.hbhagat.redis_clone;

import com.example.hbhagat.redis_clone.Server.ServerShutdownManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class })
@EnableScheduling
public class RedisCloneApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(RedisCloneApplication.class, args);

		ServerShutdownManager shutdownManager = context.getBean(ServerShutdownManager.class);

		/*
		 * Redis server has its own lifecycle because the TCP
		 * server is not an embedded Spring Web server.
		 *
		 * Spring will still close the ApplicationContext using
		 * its own shutdown hook.
		 *
		 * This hook only coordinates our Redis TCP server and
		 * AOF resources.
		 */
		Runtime.getRuntime().addShutdownHook(new Thread(shutdownManager::shutdown, "redis-shutdown-hook"));
	}
}
