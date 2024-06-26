package cmd

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleSet(connection net.Conn, server *typestructs.Server, parts []string, setGetMap map[string]string, expiryMap map[string]time.Time, connAndCommands map[net.Conn][]string, dataStr string, flag bool) string {

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

	if len(parts) < 7 {
		if flag {
			_, err := connection.Write([]byte("-ERR wrong number of arguments for command\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}
		return "-ERR wrong number of arguments for command\r\n";
	}
			
	key	:= parts[4];
	value := parts[6];

	setGetMap[key] = value;

	if len(parts) == 11 {
		expiry, err := strconv.Atoi(parts[10]);
		if err != nil {
			fmt.Println("Error converting expiry to int, may be enter valid expiry?");
			return "null"
		}

		if strings.ToLower(parts[8]) == "px" {
			expiryMap[key] = time.Now().Add(time.Duration(expiry) * time.Millisecond);
		} else if strings.ToLower(parts[8]) == "ex" {
			expiryMap[key] = time.Now().Add(time.Duration(expiry) * time.Second);
		} else {
			fmt.Println("Invalid expiry type; use PX for milliseconds or EX for seconds");
			return "null";
		}
	

		if server.Role == "master" {
			dataToSendSlave := "*3\r\n$3\r\nset\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n" + "$" + strconv.Itoa(len(parts[8])) + "\r\n" + parts[8] + "\r\n" + "$" + strconv.Itoa(len(parts[10])) + "\r\n" + parts[10] + "\r\n";
			
			for _, conn := range server.OtherServersConn {
				conn.Write([]byte(dataToSendSlave));
			}
		}

	} else {
		if server.Role == "master" {
			dataToSendSlave := "*3\r\n$3\r\nset\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n";
		
			for _, conn := range server.OtherServersConn {
				conn.Write([]byte(dataToSendSlave));
			}
		}
	}

	for _, conn := range server.OtherServersConn {
		if conn == connection {
			// dont return ok but still add to the offset
			return "null";
		}
	}

	if flag {
		_, err := connection.Write([]byte("+OK\r\n"));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	}
	return "+OK\r\n";
}