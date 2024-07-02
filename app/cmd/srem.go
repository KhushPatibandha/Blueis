package cmd

import (
	"fmt"
	"net"
	"strconv"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleSrem(connection net.Conn, server *typestructs.Server, parts []string, setMap map[string]map[string]string, hashMap map[string]map[string]string, listMap map[string][]string, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {
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

	setKeyName := parts[4];
	valueMap, ok := setMap[setKeyName];
	if !ok {
		_, ok = hashMap[setKeyName];
		_, ok1 := listMap[setKeyName];
		if ok || ok1 {
			if flag {
				_, err := connection.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
			}
			return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
		}
		
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
		member := parts[i];
		_, ok := valueMap[member];
		if ok {
			count++;
			delete(valueMap, member);
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