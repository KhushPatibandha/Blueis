package cmd

import (
	"fmt"
	"net"
	"strconv"
)

func HandleHgetall(connection net.Conn, parts []string, hashMap map[string]map[string]string, setMap map[string]map[string]string, listMap map[string][]string, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {
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

	hashKeyName := parts[4];
	valueMap, ok := hashMap[hashKeyName];
	if !ok {
		_, ok = setMap[hashKeyName];
		_, ok1 := listMap[hashKeyName];
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
			_, err := connection.Write([]byte("*0\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}
		return "*0\r\n";
	}

	dataToSend := "*" + strconv.Itoa(len(valueMap) * 2) + "\r\n";
	for key, value := range valueMap {
		dataToSend += "$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n";
		dataToSend += "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n";
	}

	if flag {
		_, err := connection.Write([]byte(dataToSend));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	}
	return dataToSend;
}
