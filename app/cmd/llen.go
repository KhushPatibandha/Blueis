package cmd

import (
	"fmt"
	"net"
	"strconv"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleLlen(connection net.Conn, server *typestructs.Server, parts []string, listMap map[string][]string, hashMap map[string]map[string]string, setMap map[string]map[string]string, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {
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

	listName := parts[4];
	valueList, ok := listMap[listName];
	if !ok {
		_, ok := hashMap[listName];
		_, ok1 := setMap[listName];
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
			return ":0\r\n";
		}
	}
	dataToSend := ":" + strconv.Itoa(len(valueList)) + "\r\n";
	if flag {
		_, err := connection.Write([]byte(dataToSend));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	}
	return dataToSend;
}