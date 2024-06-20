package cmd

import (
	"fmt"
	"net"
)

func HandleMulti(connection net.Conn, connAndCommands map[net.Conn][]string) {
	
	connAndCommands[connection] = []string{};

	_, err := connection.Write([]byte("+OK\r\n"));
	if err != nil {
		fmt.Println("Error writing:", err.Error());
	}
}