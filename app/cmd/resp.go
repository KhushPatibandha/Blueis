package cmd

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

var streamData = make(map[string][]typestructs.StreamEntry);
var setGetMap = make(map[string]string);
var expiryMap = make(map[string]time.Time);
var listMap = make(map[string][]string);
var hashMap = make(map[string]map[string]string);
var setMap = make(map[string]map[string]string);
var connAndCommands = make(map[net.Conn][]string);

/*
	For anyone looking at this code, the flag variable is just the result of my stupidity.
	i could have handled it better but here we are.
	flag = true, means write to the connection and flag = false means return the data.
*/

func ParseData(data []byte, connection net.Conn, server *typestructs.Server, ackCount *int, dir string, dbfilename string, flag bool) string {
	var dataToReturn string;
	if data[0] == '$' {
		handleBulkStrings(data);
	} else if data[0] == '*' {
		dataToReturn = handleArray(data, connection, server, ackCount, dir, dbfilename, flag);
	}

	return dataToReturn;
}

func handleArray(data []byte, connection net.Conn, server *typestructs.Server, ackCount *int, dir string, dbfilename string, flag bool) string {

	var dataToReturn string;

	dataStr := string(data);
	parts := strings.Split(dataStr, "\r\n");
	parts = parts[:len(parts) - 1];
	if len(parts) == 1 && parts[0] == "*" {
		return "error";
	}
	fmt.Println("parts: ", parts);
	numberOfElements, _ := strconv.Atoi(strings.Split(parts[0], "*")[1]);
	actualNumberOfElements := (len(parts)) / 2;

	if numberOfElements != actualNumberOfElements {
        fmt.Println("Error: Number of elements does not match")
        return "error"
    } else if numberOfElements == 1 {
		wordLen, _ := strconv.Atoi(strings.Split(parts[1], "$")[1]);
        actualWordLen := len(parts[2]);
        if wordLen != actualWordLen {
            fmt.Println("Error: Word length does not match")
            return "error"
        }

		if strings.ToLower(parts[2]) == "ping" {

			HandlePing(connection, server);
		
		} else if strings.ToLower(parts[2]) == "multi" {

			HandleMulti(connection, connAndCommands);

		} else if strings.ToLower(parts[2]) == "exec" {

			HandleExec(connection, server, connAndCommands, dir, dbfilename, ackCount);

		} else if strings.ToLower(parts[2]) == "discard" {

			HandleDiscard(connection, connAndCommands);

		} else {
			dataToSend := "-ERR unknown command '" + parts[2] + "'\r\n";
			if flag {
				_, err := connection.Write([]byte(dataToSend));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
			}
			return dataToSend;
		}
	} else {
		for i := 1; i < len(parts); i += 2 {
			wordLen, _ := strconv.Atoi(strings.Split(parts[i], "$")[1]);
			actualWordLen := len(parts[i+1]);
			if wordLen != actualWordLen {
				fmt.Println("Error: Word length does not match")
				return "error"
			}
		}
		if strings.ToLower(parts[2]) == "echo" {

			HandleEcho(connection, parts);

		} else if strings.ToLower(parts[2]) == "set" {
			
			dataToReturn = HandleSet(connection, server, parts, setGetMap, expiryMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "mset" {

			dataToReturn = HandleMset(connection, server, parts, setGetMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "incr" {

			dataToReturn = HandleIncr(connection, server, parts, setGetMap, expiryMap, connAndCommands, dataStr, dir, dbfilename, flag);

		} else if strings.ToLower(parts[2]) == "incrby" {

			dataToReturn = HandleIncrby(connection, server, parts, setGetMap, expiryMap, connAndCommands, dataStr, dir, dbfilename, flag);

		} else if strings.ToLower(parts[2]) == "decrby" {

			dataToReturn = HandleDecrby(connection, server, parts, setGetMap, expiryMap, connAndCommands, dataStr, dir, dbfilename, flag);

		} else if strings.ToLower(parts[2]) == "decr" {

			dataToReturn = HandleDecr(connection, server, parts, setGetMap, expiryMap, connAndCommands, dataStr, dir, dbfilename, flag);

		} else if strings.ToLower(parts[2]) == "get" {

			dataToReturn = HandleGet(connection, parts, setGetMap, expiryMap, dataStr, dir, dbfilename, flag);

		} else if strings.ToLower(parts[2]) == "mget" {

			dataToReturn = HandleMget(connection, parts, setGetMap, expiryMap, dataStr, dir, dbfilename, flag);

		} else if strings.ToLower(parts[2]) == "append" {

			dataToReturn = HandleAppend(connection, server, parts, setGetMap, expiryMap, connAndCommands, dataStr, dir, dbfilename, flag);

		} else if strings.ToLower(parts[2]) == "del" {

			dataToReturn = HandleDel(connection, server, parts, setGetMap, expiryMap, listMap, hashMap, setMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "exists" {

			dataToReturn = HandleExists(connection, server, parts, setGetMap, expiryMap, listMap, hashMap, setMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "info" {
			
			HandleInfo(connection, server, parts);

		} else if strings.ToLower(parts[2]) == "replconf" {
			
			HandleReplconf(connection, server, parts, dataStr);

		} else if strings.ToLower(parts[2]) == "psync" && strings.ToLower(parts[4]) == "?" && strings.ToLower(parts[6]) == "-1" {
			
			HandlePsync(connection, server);

		} else if strings.ToLower(parts[2]) == "wait" {
			
			HandleWait(connection, server, parts, ackCount);

		} else if strings.ToLower(parts[2]) == "config" {

			HandleConfig(connection, parts, dir, dbfilename);

		} else if strings.ToLower(parts[2]) == "type" {

			HandleType(connection, parts, streamData, setGetMap, expiryMap, listMap, hashMap, setMap);

		} else if strings.ToLower(parts[2]) == "xadd" {

			HandleXadd(connection, parts, streamData);

		} else if strings.ToLower(parts[2]) == "xrange" {

			HandleXrange(connection, parts, streamData);

		} else if strings.ToLower(parts[2]) == "xread" {

			HandleXread(connection, parts, streamData);

		} else if strings.ToLower(parts[2]) == "keys" {

			HandleKeys(connection, parts, dir, dbfilename);

		} else if strings.ToLower(parts[2]) == "lpush" {

			dataToReturn = HandleLpush(connection, server, parts, listMap, hashMap, setMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "lpop" {

			dataToReturn = HandleLpop(connection, server, parts, listMap, hashMap, setMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "rpush" {

			dataToReturn = HandleRpush(connection, server, parts, listMap, hashMap, setMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "rpop" {

			dataToReturn = HandleRpop(connection, server, parts, listMap, hashMap, setMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "lrange" {

			dataToReturn = HandleLrange(connection, parts, listMap, hashMap, setMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "llen" {

			dataToReturn = HandleLlen(connection, server, parts, listMap, hashMap, setMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "hset" {

			dataToReturn = HandleHset(connection, server, parts, hashMap, setMap, listMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "hget" {

			dataToReturn = HandleHget(connection, parts, hashMap, setMap, listMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "hgetall" {

			dataToReturn = HandleHgetall(connection, parts, hashMap, setMap, listMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "hdel" {

			dataToReturn = HandleHdel(connection, server, parts, hashMap, setMap, listMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "hmget" {

			dataToReturn = HandleHmget(connection, parts, hashMap, setMap, listMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "sadd" {

			dataToReturn = HandleSadd(connection, server, parts, setMap, hashMap, listMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "smembers" {

			dataToReturn = HandleSmembers(connection, parts, setMap, hashMap, listMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "sismember" {

			dataToReturn = HandleSismember(connection, parts, setMap, hashMap, listMap, connAndCommands, dataStr, flag);

		} else if strings.ToLower(parts[2]) == "srem" {

			dataToReturn = HandleSrem(connection, server, parts, setMap, hashMap, listMap, connAndCommands, dataStr, flag);

		} else {
			dataToSend := "-ERR unknown command '" + parts[2] + "'\r\n";
			if flag {
				_, err := connection.Write([]byte(dataToSend));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
			}
			return dataToSend;
		}
    }

	return dataToReturn;
}

func handleBulkStrings(date []byte) {

}

// RESP data type		Minimal protocol version	Category	First byte
// Simple strings		RESP2						Simple		+
// Simple Errors		RESP2						Simple		-
// Integers				RESP2						Simple		:
// Bulk strings			RESP2						Aggregate	$
// Arrays				RESP2						Aggregate	*
// Nulls				RESP3						Simple		_
// Booleans				RESP3						Simple		#
// Doubles				RESP3						Simple		,
// Big numbers			RESP3						Simple		(
// Bulk errors			RESP3						Aggregate	!
// Verbatim strings		RESP3						Aggregate	=
// Maps					RESP3						Aggregate	%
// Sets					RESP3						Aggregate	~
// Pushes				RESP3						Aggregate	>
