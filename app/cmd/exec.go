package cmd

import (
	"fmt"
	"net"

	typestructs "github.com/codecrafters-io/redis-starter-go/typeStructs"
)

func HandleExec(connection net.Conn, server *typestructs.Server, connAndCommands map[net.Conn][]string, dir string, dbfilename string, ackCount *int) {
	
	commands, ok := connAndCommands[connection];
	if !ok {
		_, err := connection.Write([]byte("-ERR EXEC without MULTI\r\n"));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
		return;
	}

	_, err := connection.Write([]byte("+OK\r\n"));
	if err != nil {
		fmt.Println("Error writing:", err.Error());
	}

	for _, command := range commands {
		// execute all the commands
		ParseData([]byte(command), connection, server, ackCount, dir, dbfilename);
	}

	delete(connAndCommands, connection);
}