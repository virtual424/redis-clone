package com.example.hbhagat.redis_clone.Command.handler;

import com.example.hbhagat.redis_clone.Command.CommandHandler;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.RespConstants;
import com.example.hbhagat.redis_clone.Protocol.RespError;
import org.springframework.stereotype.Component;

@Component
public final class SLEEP implements CommandHandler {

	@Override
	public String command() {
		return "SLEEP";
	}

	@Override
	public byte[] execute(RedisCommand command) {
		if (command.size() != 1) {
			return new RespError("ERR wrong number of arguments for 'SLEEP' command").encode();
		}

		final long durationSeconds;

		try {
			durationSeconds = Long.parseLong(command.argument(0));
		} catch (NumberFormatException e) {
			return new RespError("ERR value is not an integer or out of range").encode();
		}

		/*
		 * A negative sleep duration has no useful meaning.
		 * Treat it as an invalid duration rather than allowing
		 * Thread.sleep() to throw IllegalArgumentException.
		 */
		if (durationSeconds < 0) {
			return new RespError("ERR invalid sleep duration").encode();
		}

		/*
		 * Avoid multiplying seconds by 1000 manually because
		 * large values can overflow.
		 */
		final long durationMillis;

		try {
			durationMillis = Math.multiplyExact(durationSeconds, 1_000L);
		} catch (ArithmeticException e) {
			return new RespError("ERR value is not an integer or out of range").encode();
		}

		try {
			Thread.sleep(durationMillis);
		} catch (InterruptedException e) {
			/*
			 * Restore the interruption status.
			 *
			 * The command itself does not own the thread lifecycle,
			 * so we should not silently swallow interruption.
			 */
			Thread.currentThread().interrupt();

			return new RespError("ERR command interrupted").encode();
		}

		return RespConstants.OK;
	}
}
