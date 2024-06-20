package cmd

import (
	"fmt"
	"net"
)

func HandleMulti(connection net.Conn) {
	_, err := connection.Write([]byte("+OK\r\n"));
	if err != nil {
		fmt.Println("Error writing:", err.Error());
	}
}