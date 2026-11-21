package com.example.hbhagat.redis_clone.Protocol;

public final class RespConstants {

	private RespConstants() {}

	public static final byte[] OK = new RespSimpleString("OK").encode();
	public static final byte[] NIL = new RespBulkString(null).encode();
	public static final byte[] ZERO = new RespInteger(0).encode();
	public static final byte[] ONE = new RespInteger(1).encode();
	public static final byte[] MINUS_ONE = new RespInteger(-1).encode();
	public static final byte[] MINUS_TWO = new RespInteger(-2).encode();
}
