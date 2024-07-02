package cmd

import (
	"fmt"
	"net"
	"strconv"
)

func HandleHget(connection net.Conn, parts []string, hashMap map[string]map[string]string, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {
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
			_, err := connection.Write([]byte("$-1\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}
		return "$-1\r\n";
	}

	field := parts[6]
	value, ok := valueMap[field]
	if ok {
		dataToSend := "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
		if flag {
			_, err := connection.Write([]byte(dataToSend))
			if err != nil {
				fmt.Println("Error writing:", err.Error())
			}
		}
		return dataToSend
	}

	if flag {
		_, err := connection.Write([]byte("$-1\r\n"));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	}
	return "$-1\r\n";
}