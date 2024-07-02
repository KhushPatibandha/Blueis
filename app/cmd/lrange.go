package cmd

import (
	"fmt"
	"net"
	"strconv"
)

func HandleLrange(connection net.Conn, parts []string, listMap map[string][]string, hashMap map[string]map[string]string, setMap map[string]map[string]string, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {
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

	if len(parts) < 9 {
		dataToSend := "-ERR wrong number of arguments for command\r\n";
		if flag {
			_, err := connection.Write([]byte(dataToSend));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}
		return dataToSend;
	}

	listName := parts[4];
	start, _ := strconv.Atoi(parts[6]);
	end, _ := strconv.Atoi(parts[8]);
	valueList, ok := listMap[listName];
	if start > end || !ok || start >= len(valueList) {
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
			_, err := connection.Write([]byte("*0\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}
		return "*0\r\n";
	}

	if start < 0 {
		start = len(valueList) + start;
		if start < 0 {
			start = 0;
		}
	}
	if end < 0 {
		end = len(valueList) + end;
		if end < 0 {
			end = 0;
		}
	}
	if end >= len(valueList) {
		end = len(valueList) - 1;
	}

	dataToSend := "*" + strconv.Itoa(end - start + 1) + "\r\n";
	for i := start; i <= end; i++ {
		dataToSend += "$" + strconv.Itoa(len(valueList[i])) + "\r\n" + valueList[i] + "\r\n";
	}
	if flag {
		_, err := connection.Write([]byte(dataToSend));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	}
	return dataToSend;
}