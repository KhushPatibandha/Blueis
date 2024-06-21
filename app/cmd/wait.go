package cmd

import (
	"fmt"
	"net"
	"strconv"
	"time"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleWait(connection net.Conn, server *typestructs.Server, parts []string, ackCount *int) {
	replicasToWaitFor := parts[4];

	if replicasToWaitFor == "0" {
		dataToSend := ":0\r\n";
		_, err := connection.Write([]byte(dataToSend));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}
	}

	for _, conn := range server.OtherServersConn {
		conn.Write([]byte("*3\r\n$8\r\nreplconf\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"));
	}

	timeToWait := parts[6];
	timeToWaitInt, _ := strconv.Atoi(timeToWait);
	time.Sleep(time.Duration(timeToWaitInt) * time.Millisecond);
	
	replicaCount := *ackCount;
	if *ackCount == 0 {
		replicaCount = len(server.OtherServersConn);
	}
	dataToSend := ":" + strconv.Itoa(replicaCount) + "\r\n";
	_, err := connection.Write([]byte(dataToSend));
	if err != nil {
		fmt.Println("Error writing:", err.Error());
	}
	*ackCount = 0;
}