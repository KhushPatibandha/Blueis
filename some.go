package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
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
	// data := []byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
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
	data := []byte("*2\r\n$4\r\nkeys\r\n$1\r\n*\r\n");
	// data := []byte("*5\r\n$4\r\nXADD\r\n$10\r\nstream_key\r\n$3\r\n5-*\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
	// data := []byte("*5\r\n$4\r\nXADD\r\n$10\r\nstream_key\r\n$1\r\n*\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");

	command := strings.Split(string(data), "*");
	// fmt.Println(command);
	// fmt.Println(len(command));

	for i := 1; i < len(command); i++ {
		if strings.TrimSpace(command[i]) == "" {
			continue
		}

		if strings.ToLower(command[i]) == "3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n" || strings.ToLower(command[i]) == "2\r\n$4\r\nkeys\r\n$1\r\n" {
			command[i] = "*" + command[i] + "*\r\n"
		} else if strings.Contains(strings.ToLower(command[i]), "xadd") {
			command[i] = "*" + command[i]
			parts := strings.Split(command[i], "\r\n")
			if strings.HasSuffix(parts[len(parts) - 1], "-") || len(parts) == 7 {
				// get the next element in the array and append it to the current element
				command[i] = command[i] + "*" + command[i + 1];
				handleArray([]byte(command[i]));
				i++;
				continue;
			}
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

				// key, value := readFile("../../dump.rdb");
				// if key == parts[4] {
				// 	dataToSend := "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n";
				// 	fmt.Println(dataToSend)
				// 	return;	
				// }
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
				filePath := "../../dump.rdb"
				file, err := os.Open(filePath)
				if err != nil {
					fmt.Println("Error opening RDB file:", err)
					return
				}
				defer file.Close()

				// Skip the header
				file.Seek(9, 0)

				keyValues, err := readAllKeyValues(file)
				if err != nil {
					fmt.Println("Error reading key values:", err)
					return
				}

				for key, kv := range keyValues {
					if kv.ExpiryTime != nil {
						fmt.Printf("Key: %s, Value: %s, Expiry: %s\n", key, kv.Value, kv.ExpiryTime.String())
					} else {
						fmt.Printf("Key: %s, Value: %s, No Expiry\n", key, kv.Value)
					}
				}
			}
		}
    }
}

func readSizeEncoding(file *os.File) (int, error) {
	var firstByte byte
	err := binary.Read(file, binary.LittleEndian, &firstByte)
	if err != nil {
		return 0, err
	}

	switch firstByte >> 6 {
	case 0b00:
		return int(firstByte & 0x3F), nil
	case 0b01:
		var secondByte byte
		err := binary.Read(file, binary.BigEndian, &secondByte)
		if err != nil {
			return 0, err
		}
		return int(firstByte&0x3F)<<8 | int(secondByte), nil
	case 0b10:
		var size int32
		err := binary.Read(file, binary.BigEndian, &size)
		if err != nil {
			return 0, err
		}
		return int(size), nil
	case 0b11:
		return int(firstByte), nil
	}
	return 0, fmt.Errorf("invalid size encoding: 0x%x", firstByte)
}

func readStringEncoding(file *os.File) (string, error) {
	size, err := readSizeEncoding(file)
	if err != nil {
		return "", err
	}

	switch size & 0xC0 {
	case 0x00, 0x40, 0x80:
		data := make([]byte, size)
		_, err = file.Read(data)
		if err != nil {
			return "", err
		}
		return string(data), nil
	case 0xC0:
		switch size & 0x3F {
		case 0x00:
			var value int8
			err = binary.Read(file, binary.LittleEndian, &value)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", value), nil
		case 0x01:
			var value int16
			err = binary.Read(file, binary.LittleEndian, &value)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", value), nil
		case 0x02:
			var value int32
			err = binary.Read(file, binary.LittleEndian, &value)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", value), nil
		case 0x03:
			// LZF compression not supported
			return "", fmt.Errorf("LZF compression is not supported")
		}
	}

	return "", fmt.Errorf("unsupported string encoding: 0x%x", size)
}

type KeyValue struct {
	Value       string
	ExpiryTime  *time.Time
}

func readAllKeyValues(file *os.File) (map[string]KeyValue, error) {
	keyValueMap := make(map[string]KeyValue)

	for {
		var flag byte
		err := binary.Read(file, binary.LittleEndian, &flag)
		if err != nil {
			return nil, err
		}

		switch flag {
		case 0xFA:
			// Read auxiliary field (ignore content)
			_, err := readStringEncoding(file) // key
			if err != nil {
				return nil, err
			}
			_, err = readStringEncoding(file) // value
			if err != nil {
				return nil, err
			}
		case 0xFE:
			// Read database selector (ignore)
			_, err := readSizeEncoding(file)
			if err != nil {
				return nil, err
			}
		case 0xFB:
			// Read resizedb field (ignore sizes)
			_, err := readSizeEncoding(file)
			if err != nil {
				return nil, err
			}
			_, err = readSizeEncoding(file)
			if err != nil {
				return nil, err
			}
		case 0xFC:
			// Expiry time in milliseconds
			var expiryMs int64
			err := binary.Read(file, binary.LittleEndian, &expiryMs)
			if err != nil {
				return nil, err
			}
			expiryTime := time.Unix(0, expiryMs*int64(time.Millisecond))

			// Read key-value pair
			var valueType byte
			err = binary.Read(file, binary.LittleEndian, &valueType)
			if err != nil {
				return nil, err
			}
			key, err := readStringEncoding(file)
			if err != nil {
				return nil, err
			}
			value, err := readStringEncoding(file)
			if err != nil {
				return nil, err
			}
			keyValueMap[key] = KeyValue{Value: value, ExpiryTime: &expiryTime}
		case 0xFD:
			// Expiry time in seconds
			var expirySeconds int32
			err := binary.Read(file, binary.LittleEndian, &expirySeconds)
			if err != nil {
				return nil, err
			}
			expiryTime := time.Unix(int64(expirySeconds), 0)

			// Read key-value pair
			var valueType byte
			err = binary.Read(file, binary.LittleEndian, &valueType)
			if err != nil {
				return nil, err
			}
			key, err := readStringEncoding(file)
			if err != nil {
				return nil, err
			}
			value, err := readStringEncoding(file)
			if err != nil {
				return nil, err
			}
			keyValueMap[key] = KeyValue{Value: value, ExpiryTime: &expiryTime}
		case 0xFF:
			// End of file
			return keyValueMap, nil
		default:
			// Read key-value pair without expiry
			key, err := readStringEncoding(file)
			if err != nil {
				return nil, err
			}
			value, err := readStringEncoding(file)
			if err != nil {
				return nil, err
			}
			keyValueMap[key] = KeyValue{Value: value, ExpiryTime: nil}
		}
	}
}