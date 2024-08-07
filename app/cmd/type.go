package cmd

import (
	"fmt"
	"net"
	"time"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleType(connection net.Conn, parts []string, streamData map[string][]typestructs.StreamEntry, setGetMap map[string]string, expiryMap map[string]time.Time, listMap map[string][]string, hashMap map[string]map[string]string, setMap map[string]map[string]string) {
	_, ok := streamData[parts[4]];
	if ok {
		_, err := connection.Write([]byte("+stream\r\n"));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
		return;
	}

	_, ok = setGetMap[parts[4]];
	if ok {
		expiry, ok := expiryMap[parts[4]];

		if ok && time.Now().After(expiry) {
			delete(setGetMap, parts[4]);
			delete(expiryMap, parts[4]);

			_, err := connection.Write([]byte("+none\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}	

			return;
		}

		_, err := connection.Write([]byte("+string\r\n"));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
		return;
	} else {

		_, ok = listMap[parts[4]];
		if ok {
			_, err := connection.Write([]byte("+list\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
			return;
		}

		_, ok = hashMap[parts[4]];
		if ok {
			_, err := connection.Write([]byte("+hash\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
			return;
		}

		_, ok = setMap[parts[4]];
		if ok {
			_, err := connection.Write([]byte("+set\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
			return;
		}

		_, err := connection.Write([]byte("+none\r\n"));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
		return;
	}
}