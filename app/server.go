package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close();

	for {
		connection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(connection);
	}
}

func handleConnection(connection net.Conn) {
	defer connection.Close();
	for  {
		buf := make([]byte, 1024);
		_, err := connection.Read(buf);
		if err != nil {
			fmt.Println("Error reading:", err.Error());
		}
		
		fmt.Printf("Received data: %v", string(buf));
		handlePing(connection);
	}
}

func handlePing(connection net.Conn) {
	_, err := connection.Write([]byte("+PONG\r\n"));
	if err != nil {
		fmt.Println("Error writing:", err.Error());
	}
}
