package cmd

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	typestructs "github.com/codecrafters-io/redis-starter-go/typeStructs"
)

func HandleReplconf(connection net.Conn, server *typestructs.Server, parts []string, dataStr string) {
	if strings.ToLower(parts[4]) == "listening-port" {
		_, err := connection.Write([]byte("+OK\r\n"));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	} else if strings.ToLower(parts[4]) == "capa" {
		_, err := connection.Write([]byte("+OK\r\n"));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	} else if strings.ToLower(parts[4]) == "getack" && strings.ToLower(parts[6]) == "*" {
		serverOffset := server.Offset;
		respToSend := "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + strconv.Itoa(len(strconv.Itoa(serverOffset))) + "\r\n" + strconv.Itoa(serverOffset) + "\r\n"
		_, err := connection.Write([]byte(respToSend))
		if err != nil {
			fmt.Println("Error writing:", err.Error())
		}
		server.Offset += len(dataStr);
	}
}