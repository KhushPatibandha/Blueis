package cmd

import (
	"fmt"
	"net"
	"strconv"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleHdel(connection net.Conn, server *typestructs.Server, parts []string, hashMap map[string]map[string]string, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {
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

	if partsLen < 6 {
		if flag {
			_, err := connection.Write([]byte("-ERR wrong number of arguments for command\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}
		return "-ERR wrong number of arguments for command\r\n";
	}

	hashKeyName := parts[4];
	valueMap, ok := hashMap[hashKeyName];
	if !ok {
		if flag {
			_, err := connection.Write([]byte(":0\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}
		return ":0\r\n";
	}

	count := 0;
	for i := 6; i < partsLen; i+=2 {
		field := parts[i];
		_, ok := valueMap[field];
		if ok {
			delete(valueMap, field);
			count++;
		}
	}

	dataToSend := ":" + strconv.Itoa(count) + "\r\n";
	if flag {
		_, err := connection.Write([]byte(dataToSend));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	}
	return dataToSend;
}