package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

var setGetMap = make(map[string]string);
var expiryMap = make(map[string]time.Time)

func ParseData(data []byte, connection net.Conn) {
	if data[0] == '$' {
		handleBulkStrings(data);
	} else if data[0] == '*' {
		handleArray(data, connection);
	}
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
			_, err := connection.Write([]byte("+PONG\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
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
			_, err := connection.Write([]byte(dataToEcho));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		} else if strings.ToLower(parts[2]) == "set" {
			
			key	:= parts[4];
			value := parts[6];

			setGetMap[key] = value;

			if len(parts) == 12 {
				expiry, err := strconv.Atoi(parts[10]);
				if err != nil {
					fmt.Println("Error converting expiry to int, may be enter valid expiry?");
				}

				if strings.ToLower(parts[8]) == "px" {
					expiryMap[key] = time.Now().Add(time.Duration(expiry) * time.Millisecond);
				} else if strings.ToLower(parts[8]) == "ex" {
					expiryMap[key] = time.Now().Add(time.Duration(expiry) * time.Second);
				} else {
					fmt.Println("Invalid expiry type; use PX for milliseconds or EX for seconds");
				}
			}

			_, err := connection.Write([]byte("+OK\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
			
		} else if strings.ToLower(parts[2]) == "get" {
			keyToGet, ok := setGetMap[parts[4]];
			if ok {
				expiry, ok := expiryMap[parts[4]];

				if ok && time.Now().After(expiry) {
					delete(setGetMap, parts[4]);
					delete(expiryMap, parts[4]);

					_, err := connection.Write([]byte("$-1\r\n"));
					if err != nil {
						fmt.Println("Error writing:", err.Error());
					}	

					return;
				}

				dataToSend := "$" + strconv.Itoa(len(keyToGet)) + "\r\n" + keyToGet + "\r\n";
				_, err := connection.Write([]byte(dataToSend));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
			} else {
				_, err := connection.Write([]byte("$-1\r\n"));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
			}
		} else if strings.ToLower(parts[2]) == "info" {
			if strings.ToLower(parts[4]) == "replication" {
				role := "";
				replId = GetReplId();
				replOffset := "0";

				if GetRole() == "slave" {
					role = "role:slave";
				} else {
					role = "role:master";
				}

				dataToSend := "role:" + role + "\r\n" +
							"master_replid:" + replId + "\r\n" +
							"master_repl_offset:" + replOffset + "\r\n"

				respToSend := "$" + strconv.Itoa(len(dataToSend)) + "\r\n" + dataToSend + "\r\n"

				_, err := connection.Write([]byte(respToSend))
				if err != nil {
					fmt.Println("Error writing:", err.Error())
				}
			}
		} else if strings.ToLower(parts[2]) == "replconf" {
			if strings.ToLower(parts[4]) == "listening-port" {
				_, err := connection.Write([]byte("+OK\r\n"));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
			} else if strings.ToLower(parts[4]) == "capa" {
				_, err := connection.Write([]byte("+OK\r\n"));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
			}
		}
    }
}

func handleBulkStrings(date []byte) {
}

// RESP data type		Minimal protocol version	Category	First byte
// Simple strings		RESP2						Simple		+
// Simple Errors		RESP2						Simple		-
// Integers				RESP2						Simple		:
// Bulk strings			RESP2						Aggregate	$
// Arrays				RESP2						Aggregate	*
// Nulls				RESP3						Simple		_
// Booleans				RESP3						Simple		#
// Doubles				RESP3						Simple		,
// Big numbers			RESP3						Simple		(
// Bulk errors			RESP3						Aggregate	!
// Verbatim strings		RESP3						Aggregate	=
// Maps					RESP3						Aggregate	%
// Sets					RESP3						Aggregate	~
// Pushes				RESP3						Aggregate	>
