package cmd

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func HandleConfig(connection net.Conn, parts []string, dir string, dbfilename string) {
	if strings.ToLower(parts[4]) == "get" {
		if strings.ToLower(parts[6]) == "dir" {
			dataToSend := "*2\r\n$3\r\ndir\r\n$" + strconv.Itoa(len(dir)) + "\r\n" + dir + "\r\n";
			_, err := connection.Write([]byte(dataToSend));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		} else if strings.ToLower(parts[6]) == "dbfilename" {
			dataToSend := "*2\r\n$10\r\ndbfilename\r\n$" + strconv.Itoa(len(dbfilename)) + "\r\n" + dbfilename + "\r\n";
			_, err := connection.Write([]byte(dataToSend));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}
	}
}