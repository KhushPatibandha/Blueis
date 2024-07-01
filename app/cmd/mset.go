package cmd

import (
	"fmt"
	"net"
	"strconv"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleMset(connection net.Conn, server *typestructs.Server, parts []string, setGetMap map[string]string, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {
	if flag {
		_, ok := connAndCommands[connection];
		if ok {
			connAndCommands[connection] = append(connAndCommands[connection], dataStr);
			
			_, err := connection.Write([]byte("+QUEUED\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
			return "+QUEUED\r\n";
		}
	}

	server.Offset += len(dataStr);
	partsLen := len(parts);

	// MSET key1 "Hello" key2 "World"
	// *5\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$5\r\nHello\r\n$4\r\nkey2\r\n$5\r\nWorld\r\n
	// [*5 $4 MSET $4 key1 $5 Hello $4 key2 $5 World]
	for i := 3; i < partsLen; i += 4 {
		key := parts[i + 1];
		value := parts[i + 3];
		setGetMap[key] = value;
	}

	if server.Role == "master" {
		lenToSend := (partsLen - 1) / 2;
		dataToSendSlave := "*" + strconv.Itoa(lenToSend) + "\r\n$4\r\nMSET\r\n";
		for i := 3; i < partsLen; i++ {
			dataToSendSlave += parts[i] + "\r\n";
		}
		for _, conn := range server.OtherServersConn {
			conn.Write([]byte(dataToSendSlave));
		}
	}

	for _, conn := range server.OtherServersConn {
		if conn == connection {
			// dont return ok but still add to the offset
			return "null";
		}
	}

	if flag {
		_, err := connection.Write([]byte("+OK\r\n"));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	}
	return "+OK\r\n";
}