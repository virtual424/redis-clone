package com.example.hbhagat.redis_clone.Server;

public enum ServerState {
    WAITING,
    BUSY,
    SHUTTING_DOWN
}

//	connect redis server to my tcp server
//	redis-cli -h 172.28.176.1 -p 6369