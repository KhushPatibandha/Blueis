package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
)

var Role = "master";

func main() {
    port := flag.String("port", "6379", "port to listen on");
	replicaof := flag.String("replicaof", "", "master server to replicate from");
    flag.Parse();

	if *replicaof != "" {
		Role = "slave";
	}

    l, err := net.Listen("tcp", "0.0.0.0:"+*port)
    if err != nil {
        fmt.Println("Failed to bind to port", *port)
        os.Exit(1)
    }
    defer l.Close()

    for {
        connection, err := l.Accept()
        if err != nil {
            fmt.Println("Error accepting connection: ", err.Error())
            os.Exit(1)
        }
        go handleConnection(connection)
    }
}

func handleConnection(connection net.Conn) {
    defer connection.Close()
    for {
        buf := make([]byte, 1024)
        _, err := connection.Read(buf)
        if err != nil {
            if err == io.EOF {
                return
            }
            fmt.Println("Error reading:", err.Error())
        }
        ParseData(buf, connection)
    }
}

func GetRole() string {
	return Role;
}
