package cmd

import (
	"fmt"
	"net"
	"strconv"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
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

	if ok && len(commands) == 0 {
		_, err := connection.Write([]byte("*0\r\n"));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
		delete(connAndCommands, connection);
		return;
	}

	var returnSlice []string;

	for _, command := range commands {
		// execute all the commands
		data := ParseData([]byte(command), connection, server, ackCount, dir, dbfilename, false);
		returnSlice = append(returnSlice, data);
	}

	// write the data in an redis protocol array format
	dataToSend := "*" + strconv.Itoa(len(returnSlice)) + "\r\n";
	for _, data := range returnSlice {
		dataToSend += data;
	}

	_, err := connection.Write([]byte(dataToSend));
	if err != nil {
		fmt.Println("Error writing:", err.Error());
	}

	delete(connAndCommands, connection);
}