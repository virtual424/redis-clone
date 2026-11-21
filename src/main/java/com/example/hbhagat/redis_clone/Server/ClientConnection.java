package com.example.hbhagat.redis_clone.Server;

import com.example.hbhagat.redis_clone.Command.CommandDispatcher;
import com.example.hbhagat.redis_clone.Command.RedisCommand;
import com.example.hbhagat.redis_clone.Protocol.CommandReader;
import com.example.hbhagat.redis_clone.Protocol.RespSimpleString;
import com.example.hbhagat.redis_clone.Storage.Mutation.MutationCoordinator;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents one connected Redis client.
 *
 * Transaction state belongs exclusively to this connection.
 */
public final class ClientConnection {

	private final SocketChannel channel;

	private final FileDescriptorCommunication communication;

	private final CommandReader reader;

	private final List<RedisCommand> transactionQueue = new ArrayList<>();

	private boolean transactionActive;

	public ClientConnection(SocketChannel channel) {
		if (channel == null) {
			throw new IllegalArgumentException("channel cannot be null");
		}

		this.channel = channel;
		this.communication = new FileDescriptorCommunication(channel);
		this.reader = new CommandReader();
	}

	public SocketChannel channel() {
		return channel;
	}

	public FileDescriptorCommunication comm() {
		return communication;
	}

	public CommandReader reader() {
		return reader;
	}

	public boolean isTransactionActive() {
		return transactionActive;
	}

	public byte[] beginTransaction(RedisCommand command) {
		if (command.size() != 0) {
			return error("ERR wrong number of arguments for 'multi' command");
		}

		if (transactionActive) {
			return error("ERR MULTI calls can not be nested");
		}

		transactionActive = true;
		transactionQueue.clear();

		return new RespSimpleString("OK").encode();
	}

	public byte[] queueCommand(RedisCommand command) {
		transactionQueue.add(command);

		return new RespSimpleString("QUEUED").encode();
	}

	public byte[] executeTransaction(RedisCommand command, CommandDispatcher dispatcher, MutationCoordinator coordinator) {
		if (command.size() != 0) {
			return error("ERR wrong number of arguments for 'exec' command");
		}

		if (!transactionActive) {
			return error("ERR EXEC without MULTI");
		}

		/*
		 * Copy before changing transaction state.
		 */
		List<RedisCommand> commands = List.copyOf(transactionQueue);

		transactionQueue.clear();
		transactionActive = false;

		/*
		 * RESP array:
		 *
		 * *<count>\r\n
		 * <response 1>
		 * <response 2>
		 * ...
		 */
		/*
		 * Run all queued commands while holding the mutation
		 * lock, so background expiry cannot delete a key in the
		 * middle of the transaction. The event loop is single
		 * threaded, so other clients cannot interleave anyway.
		 */
		return coordinator.runExclusively(() -> {
			java.io.ByteArrayOutputStream response = new java.io.ByteArrayOutputStream();

			response.writeBytes(("*" + commands.size() + "\r\n").getBytes(java.nio.charset.StandardCharsets.US_ASCII));

			for (RedisCommand queuedCommand : commands) {
				byte[] commandResponse = dispatcher.dispatch(queuedCommand);

				if (commandResponse == null) {
					continue;
				}

				response.writeBytes(commandResponse);
			}

			return response.toByteArray();
		});
	}

	public byte[] discardTransaction(RedisCommand command) {
		if (command.size() != 0) {
			return error("ERR wrong number of arguments for 'discard' command");
		}

		if (!transactionActive) {
			return error("ERR DISCARD without MULTI");
		}

		transactionQueue.clear();
		transactionActive = false;

		return new RespSimpleString("OK").encode();
	}

	public void resetTransaction() {
		transactionQueue.clear();
		transactionActive = false;
	}

	private static byte[] error(String message) {
		return new com.example.hbhagat.redis_clone.Protocol.RespError(message).encode();
	}
}
