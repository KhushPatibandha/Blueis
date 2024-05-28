package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)


const (
	opCodeModuleAux    byte = 247 /* Module auxiliary data. */
	opCodeIdle         byte = 248 /* LRU idle time. */
	opCodeFreq         byte = 249 /* LFU frequency. */
	opCodeAux          byte = 250 /* RDB aux field. */
	opCodeResizeDB     byte = 251 /* Hash table resize hint. */
	opCodeExpireTimeMs byte = 252 /* Expire time in milliseconds. */
	opCodeExpireTime   byte = 253 /* Old expire time in seconds. */
	opCodeSelectDB     byte = 254 /* DB number of the following keys. */
	opCodeEOF          byte = 255
)

var setGetMap = make(map[string]string);
var expiryMap = make(map[string]time.Time)

const charset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()));

func StringWithCharset(length int, charset string) string {
    b := make([]byte, length)
    for i := range b {
        b[i] = charset[seededRand.Intn(len(charset))]
    }
    return string(b)
}

func getHash(length int) string {
    return StringWithCharset(length, charset)
}

var s = getHash(40);
func main() {
	// *1\r\n$4\r\nPING\r\n
	// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
	// *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
	// *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
	// *5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n
	// "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n"
	// *3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n
	// *3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n
	// *3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n
	// *3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n
	// *3\r\n$4\r\nWAIT\r\n$1\r\n0\r\n$5\r\n60000\r\n

	// data := []byte("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
	// data := []byte("*1\r\n$4\r\nPING\r\n");
	// data := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
	data := []byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
	// data := []byte("*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n");
	// data := []byte("*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n");
	// data := []byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n");
	// data := []byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");

	// data := []byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");

	// data := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\nbaz\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n");

	// data := []byte("*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
	
	// data := []byte("*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n");
	// data := []byte("*3\r\n$3\r\nSET\r\n$5\r\ngrape\r\n$9\r\nblueberry\r\n");
	// data := []byte("*3\r\n$3\r\nSET\r\n$5\r\ngrape\r\n$9\r\npineapple\r\n");
	// fmt.Println(len(data) + 37)
	
	// data := []byte("*3\r\n$4\r\nWAIT\r\n$1\r\n0\r\n$5\r\n60000\r\n");
	// data := []byte("*2\r\n$4\r\nkeys\r\n$1\r\n*\r\n");

	command := strings.Split(string(data), "*");
	// fmt.Println(command);
	// fmt.Println(len(command));

	for i := 1; i < len(command); i++ {
		if strings.TrimSpace(command[i]) == "" {
			continue
		}

		if strings.ToLower(command[i]) == "3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n" || strings.ToLower(command[i]) == "2\r\n$4\r\nkeys\r\n$1\r\n" {
			command[i] = "*" + command[i] + "*\r\n"
		} else {
			command[i] = "*" + command[i]
		}
		handleArray([]byte(command[i]));
	}

	// handleArray(data);

	// fmt.Println(s);
	// fmt.Println(len(s));
}

func handleArray(data []byte) {
	dataStr := string(data);
	parts := strings.Split(dataStr, "\r\n");
	parts = parts[:len(parts) - 1];
	fmt.Println(parts);
	fmt.Println("parts length: ", len(parts));
	
	numberOfElements, _ := strconv.Atoi(strings.Split(parts[0], "*")[1]);
	fmt.Println("Number of elements: ", numberOfElements);
	actualNumberOfElements := (len(parts)) / 2;
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
		for i := 1; i < len(parts); i += 2 {
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

				key, value := readFile("../../dump.rdb");
				if key == parts[4] {
					dataToSend := "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n";
					fmt.Println(dataToSend)
					return;	
				}
				// _, err := connection.Write([]byte("$-1\r\n"));
				// if err != nil {
				// 	fmt.Println("Error writing:", err.Error());
				// }
				
				fmt.Println("$-1\r");
			}
		} else if strings.ToLower(parts[2]) == "info" {
			if strings.ToLower(parts[4]) == "replication" {
				role := "master";
				replId := s;
				replOffset := "0";

				if role == "slave" {
					role = "role:slave";
				} else {
					role = "role:master";
				}

				dataToSend := "$" + strconv.Itoa(len(role)) + "\r\n" + role + "\r\n" + "$" + strconv.Itoa(len(replId) + 14) + "\r\n" + "master_replid:" + replId + "\r\n" + "$" + strconv.Itoa(len(replOffset) + 19) + "\r\n" + "master_repl_offset:" + replOffset + "\r\n";

				fmt.Println("dataToSend: ", dataToSend);
			}
		} else if strings.ToLower(parts[2]) == "keys" {
			if strings.ToLower(parts[4]) == "*" {
				filePath := "../../dump.rdb";

				fileContent, value := readFile(filePath);
				fmt.Println(fileContent)
				fmt.Println(value)
			}
		}
    }
}

func sliceIndex(data []byte, sep byte) int {
	for i, b := range data {
		if b == sep {
			return i
		}
	}
	return -1
}
func parseTable(bytes []byte) []byte {
	start := sliceIndex(bytes, opCodeResizeDB)
	end := sliceIndex(bytes, opCodeEOF)
	return bytes[start+1 : end]
}
func readFile(path string) (string, string) {
	c, _ := os.ReadFile(path)
	key := parseTable(c)
	if key == nil {
		return "", "";
	}
	str := key[4 : 4+key[3]]
	value := key[4+key[3]+1 : 4+key[3]+1+key[4+key[3]]]
	return string(str), string(value)
}
