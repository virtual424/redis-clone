package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespBulkString;
import com.example.hbhagat.redis_clone.Storage.Statistics.KeyspaceStatistics;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public final class INFO implements CommandHandler {

	private final KeyspaceStatistics statistics;

	@Override
	public String command() {
		return "INFO";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		long keys = statistics.keyCount();

		long expires = statistics.expiringKeyCount();

		long avgTtl = statistics.averageTtl();

		String info = "# Keyspace\r\n" + "db0:keys=" + keys + ",expires=" + expires + ",avg_ttl=" + avgTtl + "\r\n";

		return new RespBulkString(info.getBytes(StandardCharsets.UTF_8)).encode();
	}
}
