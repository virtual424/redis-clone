package com.example.hbhagat.redis_clone.Protocol;

public sealed interface RespValue permits RespSimpleString, RespBulkString, RespInteger, RespArray, RespError {
	byte[] encode();
}
