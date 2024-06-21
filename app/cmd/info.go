package cmd

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleInfo(connection net.Conn, server *typestructs.Server, parts []string) {
	if strings.ToLower(parts[4]) == "replication" {
		role := "";
		replOffset := "0";

		if server.Role == "slave" {
			role = "slave";
		} else {
			role = "master";
		}

		dataToSend := "role:" + role + "\r\n" + "master_replid:" + server.ReplId + "\r\n" + "master_repl_offset:" + replOffset + "\r\n"

		respToSend := "$" + strconv.Itoa(len(dataToSend)) + "\r\n" + dataToSend + "\r\n"

		_, err := connection.Write([]byte(respToSend))
		if err != nil {
			fmt.Println("Error writing:", err.Error())
		}
	}
}