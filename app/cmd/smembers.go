package cmd

import (
	"fmt"
	"net"
	"strconv"
)

func HandleSmembers(connection net.Conn, parts []string, setMap map[string]map[string]string, hashMap map[string]map[string]string, listMap map[string][]string, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {
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
			_, err := connection.Write([]byte("*0\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}
		return "*0\r\n";
	}

	dataToReturn := "*" + strconv.Itoa(len(valueMap)) + "\r\n";
	for _, value := range valueMap {
		dataToReturn += "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n";
	}

	if flag {
		_, err := connection.Write([]byte(dataToReturn));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	}
	return dataToReturn;
}