package cmd

import (
	"fmt"
	"net"
	"strconv"
	"time"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleDel(connection net.Conn, server *typestructs.Server, parts []string, setGetMap map[string]string, expiryMap map[string]time.Time, listMap map[string][]string, hashMap map[string]map[string]string, setMap map[string]map[string]string, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {
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
	count := 0;
	for i := 4; i < partsLen; i+=2 {
		_, ok := setGetMap[parts[i]];
		_, ok1 := listMap[parts[i]];
		_, ok2 := hashMap[parts[i]];
		_, ok3 := setMap[parts[i]];
		if ok || ok1 || ok2 || ok3 {
			count++;
			delete(setGetMap, parts[i]);
			delete(expiryMap, parts[i]);
			delete(listMap, parts[i]);
			delete(hashMap, parts[i]);
			delete(setMap, parts[i]);
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