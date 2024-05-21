package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func main() {
	// *1\r\n$4\r\nPING\r\n
	// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
	// data := []byte("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
	// data := []byte("*1\r\n$4\r\nPING\r\n");
	// handleArray(data);
}

func handleArray(data []byte, connection net.Conn) {
	dataStr := string(data);
	parts := strings.Split(dataStr, "\r\n");
	fmt.Println(parts);
	
	numberOfElements, _ := strconv.Atoi(strings.Split(parts[0], "*")[1]);
	actualNumberOfElements := (len(parts) - 1) / 2;

	if numberOfElements != actualNumberOfElements {
        fmt.Println("Error: Number of elements does not match")
        return
    } else if numberOfElements == 1 {
		wordLen, _ := strconv.Atoi(strings.Split(parts[1], "$")[1]);
        actualWordLen := len(parts[2]);
		actualWord := parts[2];
        if wordLen != actualWordLen {
            fmt.Println("Error: Word length does not match")
            return
        }

		if strings.ToLower(actualWord) == "ping" {
			// _, err := connection.Write([]byte("+PONG\r\n"));
			// if err != nil {
			// 	fmt.Println("Error writing:", err.Error());
			// }
			fmt.Println("+PONG");
		}
	} else {
		for i := 1; i < len(parts) - 1; i += 2 {
			wordLen, _ := strconv.Atoi(strings.Split(parts[i], "$")[1]);
			actualWordLen := len(parts[i+1]);
			fmt.Println(actualWordLen);
			if wordLen != actualWordLen {
				fmt.Println("Error: Word length does not match")
				return
			}
		}
		if strings.ToLower(parts[2]) == "echo" {
			dataToEcho := "$" + strconv.Itoa(len(parts[4])) + "\r\n" + parts[4] + "\r\n";
			// _, err := connection.Write([]byte(dataToEcho));
			// if err != nil {
			// 	fmt.Println("Error writing:", err.Error());
			// }
			fmt.Println(dataToEcho);
		}
    }
}
