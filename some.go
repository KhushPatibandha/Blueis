package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var setGetMap = make(map[string]string);
var expiryMap = make(map[string]time.Time)

func main() {
	// *1\r\n$4\r\nPING\r\n
	// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
	// *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
	// *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
	// *5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n

	// data := []byte("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
	// data := []byte("*1\r\n$4\r\nPING\r\n");
	// data := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
	data := []byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
	// data := []byte("*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n");

	handleArray(data);
}

func handleArray(data []byte) {
	dataStr := string(data);
	parts := strings.Split(dataStr, "\r\n");
	fmt.Println(parts);
	fmt.Println("parts length: ", len(parts));
	
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

			if len(parts) == 12 {
				expiry, err := strconv.Atoi(parts[10]);
				if err != nil {
					fmt.Println("Error converting expiry to int, may be enter valid expiry?");
				}

				if strings.ToLower(parts[8]) == "px" {
					expiryTime := time.Now().Add(time.Duration(expiry) * time.Millisecond);
					fmt.Println("expiry time: ", expiryTime);
					expiryMap[key] = expiryTime;
				} else if strings.ToLower(parts[8]) == "ex" {
					expiryTime := time.Now().Add(time.Duration(expiry) * time.Second);
					fmt.Println("expiry time: ", expiryTime);
					expiryMap[key] = expiryTime;
				} else {
					fmt.Println("Invalid expiry type; use PX for milliseconds or EX for seconds");
				}
			}


			// _, err := connection.Write([]byte("+OK\r\n"));
			// if err != nil {
			// 	fmt.Println("Error writing:", err.Error());
			// }
			
			fmt.Println("OK");
		} else if strings.ToLower(parts[2]) == "get" {
			keyToGet, ok := setGetMap[parts[4]];

			// key := "foo";
			// value := "bar";
			// expiry := 10;
			// // expiryTime := time.Now().Add(time.Duration(expiry) * time.Millisecond);
			// expiryTime := time.Now().Add(time.Duration(expiry) * time.Second);
			// setGetMap[key] = value;
			// expiryMap[key] = expiryTime;

			// time.Sleep(5 * time.Second);

			// keyToGet, ok := setGetMap[parts[4]];

			if ok {
				expiry, ok := expiryMap[parts[4]];

				if ok && time.Now().After(expiry) {
					delete(setGetMap, parts[4]);
					delete(expiryMap, parts[4]);

					// _, err := connection.Write([]byte("$-1\r\n"));
					// if err != nil {
					// 	fmt.Println("Error writing:", err.Error());
					// }	

					fmt.Println("$-1\r");
					return;
				}

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
				
				fmt.Println("$-1\r");
			}
		}
    }
}
