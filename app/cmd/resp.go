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
var expiryMap = make(map[string]time.Time)
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

		} else if strings.ToLower(parts[2]) == "append" {

			dataToReturn = HandleAppend(connection, server, parts, setGetMap, expiryMap, connAndCommands, dataStr, dir, dbfilename, flag);

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

			HandleType(connection, parts, streamData, setGetMap, expiryMap);

		} else if strings.ToLower(parts[2]) == "xadd" {

			HandleXadd(connection, parts, streamData);

		} else if strings.ToLower(parts[2]) == "xrange" {

			HandleXrange(connection, parts, streamData);

		} else if strings.ToLower(parts[2]) == "xread" {

			HandleXread(connection, parts, streamData);

		} else if strings.ToLower(parts[2]) == "keys" {

			HandleKeys(connection, parts, dir, dbfilename);

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
