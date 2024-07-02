package cmd

import (
	"fmt"
	"net"
	"strconv"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleRpop(connection net.Conn, server *typestructs.Server, parts []string, listMap map[string][]string, hashMap map[string]map[string]string, setMap map[string]map[string]string, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {
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
			_, err := connection.Write([]byte("$-1\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
			return "$-1\r\n";
		}
	}

	if len(parts) == 5 {
		dataToSend := "$" + strconv.Itoa(len(valueList[len(valueList) - 1])) + "\r\n" + valueList[len(valueList) - 1] + "\r\n";
		valueList = valueList[:len(valueList) - 1];
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
		for i := len(valueList) - 1; i >= 0; i-- {
			dataToSend += "$" + strconv.Itoa(len(valueList[i])) + "\r\n" + valueList[i] + "\r\n";
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

	i := valueListLen - 1;
	count := 0;
	for i >= 0 && count < targetPop {
		dataToSend += "$" + strconv.Itoa(len(valueList[i])) + "\r\n" + valueList[i] + "\r\n";
		i--;
		count++;
	}
	valueList = valueList[:i + 1];
	
	listMap[listName] = valueList;
	if flag {
		_, err := connection.Write([]byte(dataToSend));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	}

	return dataToSend;
}