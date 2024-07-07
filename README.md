# Blueis

Super fast drop-in replacement of the in memory key-value store redis in golang

## Run Locally

### Clone the project
`git clone https://github.com/KhushPatibandha/Blueis.git`

### Navigate to the project directory
`cd .\Blueis\`

### Start the server

#### Single server on default port 6379
`./spawn_redis_server.sh`

#### Single server on custom port 'X'
`./spawn_redis_server.sh --port X`

eg: `./spawn_redis_server.sh --port 5555`

#### Start master server on port 'X' along with replica server on port 'Y'
`./spawn_redis_server.sh --port Y --replicaof "localhost X"`

eg: `./spawn_redis_server.sh --port 5555 --replicaof "localhost 6666"`

Here the master server will start on port 6666 and the replica server (slave server) will start on port 5555.

Also in case if you want to start more then 1 slave server. you can run this same command multiple times with different port number for `--port` flag and same port for `--replicaof` flag.

### Flags

#### `--replicaof`
- Sets the port for master server.
- Should always be used with `--port` flag.

#### `--port`
- If used without `--replicaof` flag, sets the port for the only server (master) that is running.
- If used with `--replicaof` flag, sets the port for slave server.

#### `--dir`
- Path to the RDB file.
- Always use along with `--dbfilename`.
- Usage: `./spawn_redis_server.sh --dir /tmp/redis-files --dbfilename dump.rdb'.

#### `--dbfilename`
- Name of the RDB file.
- Always use along with `--dir`.
- Usage: `./spawn_redis_server.sh --dir /tmp/redis-files --dbfilename dump.rdb'.

### Connect to the servers
`redis-cli -p <port-number>`

eg: `redis-cli -p 6379`

## Available commands
### Misc
`PING` `ECHO` `INFO`
### Keys
`TYPE` `DEL` `EXISTS` `KEYS`
### Strings
`SET` `GET` `INCR` `DECR` `INCRBY` `DECRBY` `APPEND` `MGET` `MSET` 
### Lists
`LPUSH` `LPOP` `RPUSH` `RPOP` `LRANGE` `LLEN`
### Hashes
`HSET` `HGET` `HMGET` `HGETALL` `HDEL`
### Sets
`SADD` `SREM` `SISMEMBER` `SMEMBERS`
### Streams
`XADD` `XREAD` `XRANGE`
### Transactions
`MULTI` `EXEC` `DISCARD`
### Replication
`WAIT` `REPLCONF` `PSYNC`
### RDB Persistence
`CONFIG GET`


























