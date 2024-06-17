package cmd

import (
	"fmt"
	"net"
	"strconv"

	typestructs "github.com/codecrafters-io/redis-starter-go/typeStructs"
)

func HandleEcho(connection net.Conn, server *typestructs.Server, parts []string) {
	dataToEcho := "$" + strconv.Itoa(len(parts[4])) + "\r\n" + parts[4] + "\r\n";
	_, err := connection.Write([]byte(dataToEcho));
	if err != nil {
		fmt.Println("Error writing:", err.Error());
	}
}