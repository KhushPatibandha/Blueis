package cmd

import (
	"fmt"
	"net"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandlePing(connection net.Conn, server *typestructs.Server) {
	server.Offset += 14;

	for _, conn := range server.OtherServersConn {
		if conn == connection {
			// dont return pong but still add to the offset
			return;
		}
	}
	_, err := connection.Write([]byte("+PONG\r\n"));
	if err != nil {
		fmt.Println("Error writing:", err.Error());
	}
}