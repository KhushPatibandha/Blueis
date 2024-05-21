package main

import (
	"fmt"
	"strconv"
	"strings"
)

var setGetMap = make(map[string]string);

func main() {
	// *1\r\n$4\r\nPING\r\n
	// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
	// *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
	// *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n

	// data := []byte("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
	// data := []byte("*1\r\n$4\r\nPING\r\n");
	// data := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
	data := []byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
	handleArray(data);
}

func handleArray(data []byte) {
	dataStr := string(data);
	parts := strings.Split(dataStr, "\r\n");
	fmt.Println(parts);
	
	numberOfElements, _ := strconv.Atoi(strings.Split(parts[0], "*")[1]);
	fmt.Println("Number of elements: ", numberOfElements);
	actualNumberOfElements := (len(parts) - 1) / 2;
	fmt.Println("Actual number of elements: ", actualNumberOfElements);

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
			fmt.Println(dataToEcho);
		} else if strings.ToLower(parts[2]) == "set" {
			key := parts[4];
			value := parts[6];
				
			setGetMap[key] = value;

			fmt.Println("Key: ", key);
			fmt.Println("Value: ", setGetMap[key]);

			// _, err := connection.Write([]byte("+OK\r\n"));
			// if err != nil {
			// 	fmt.Println("Error writing:", err.Error());
			// }
			
			fmt.Println("OK");
		} else if strings.ToLower(parts[2]) == "get" {
			keyToGet, ok := setGetMap[parts[4]];
			if ok {
				dataToSend := "$" + strconv.Itoa(len(keyToGet)) + "\r\n" + keyToGet + "\r\n";
				// _, err := connection.Write([]byte(dataToSend));
				// if err != nil {
				// 	fmt.Println("Error writing:", err.Error());
				// }
				
				fmt.Println(dataToSend);
			} else {
				// _, err := connection.Write([]byte("$-1\r\n"));
				// if err != nil {
				// 	fmt.Println("Error writing:", err.Error());
				// }
				
				fmt.Println("$-1\r\n");
			}
		}
    }
}
