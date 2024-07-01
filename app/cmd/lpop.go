package cmd

import (
	"fmt"
	"net"
	"strconv"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleLpop(connection net.Conn, server *typestructs.Server, parts []string, listMap map[string][]string, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {
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

	listName := parts[4];
	valueList, ok := listMap[listName];
	if !ok {
		if flag {
			_, err := connection.Write([]byte("$-1\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
			return "$-1\r\n";
		}
	}

	if len(parts) == 5 {
		dataToSend := "$" + strconv.Itoa(len(valueList[0])) + "\r\n" + valueList[0] + "\r\n";
		valueList = valueList[1:];
		listMap[listName] = valueList;
		if flag {
			_, err := connection.Write([]byte(dataToSend));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}
		return dataToSend;
	}

	targetPop, _ := strconv.Atoi(parts[6]);
	valueListLen := len(valueList);
	if targetPop >= valueListLen {
		dataToSend := "*" + strconv.Itoa(valueListLen) + "\r\n";
		for _, value := range valueList {
			dataToSend += "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n";
		}
		delete(listMap, listName);
		if flag {
			_, err := connection.Write([]byte(dataToSend));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}
		return dataToSend;
	}

	dataToSend := "*" + strconv.Itoa(targetPop) + "\r\n";
	for i := 0; i < targetPop; i++ {
		dataToSend += "$" + strconv.Itoa(len(valueList[0])) + "\r\n" + valueList[0] + "\r\n";
		valueList = valueList[1:];
	}
	listMap[listName] = valueList;
	if flag {
		_, err := connection.Write([]byte(dataToSend));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	}

	return dataToSend;
}