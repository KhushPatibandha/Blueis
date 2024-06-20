package cmd

import (
	"fmt"
	"net"
)

func HandleDiscard(connection net.Conn, connAndCommands map[net.Conn][]string) {
	_, ok := connAndCommands[connection];

	if ok {
		delete(connAndCommands, connection);
		_, err := connection.Write([]byte("+OK\r\n"));
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error());
		}
		return;
	}
	
	if !ok {
		_, err := connection.Write([]byte("-ERR DISCARD without MULTI\r\n"));
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error());
		}
		return;
	}
}