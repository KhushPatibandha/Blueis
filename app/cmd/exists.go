package cmd

import (
	"fmt"
	"net"
	"strconv"
	"time"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleExists(connection net.Conn, server *typestructs.Server, parts []string, setGetMap map[string]string, expiryMap map[string]time.Time, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {
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

	partsLen := len(parts);
	count := 0;
	for i := 4; i < partsLen; i+=2 {
		_, ok := setGetMap[parts[i]];
		if ok {
			expiry, ok := expiryMap[parts[i]];
			if ok && time.Now().After(expiry) {
				delete(setGetMap, parts[i]);
				delete(expiryMap, parts[i]);
			} else {
				count++;
			}
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