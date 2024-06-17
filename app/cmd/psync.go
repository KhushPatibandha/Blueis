package cmd

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"

	typestructs "github.com/codecrafters-io/redis-starter-go/typeStructs"
)

func HandlePsync(connection net.Conn, server *typestructs.Server) {
	dataToSend := "+FULLRESYNC " + server.ReplId + " 0\r\n";
	_, err := connection.Write([]byte(dataToSend));
	if err != nil {
		fmt.Println("Error writing:", err.Error());
	}

	rdbHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
	rdbBytes, _ := hex.DecodeString(rdbHex);

	dataToSend = "$" + strconv.Itoa(len(rdbBytes)) + "\r\n" + string(rdbBytes);
	_, err2 := connection.Write([]byte(dataToSend));
	if err2 != nil {
		fmt.Println("Error writing:", err.Error());
	}
}