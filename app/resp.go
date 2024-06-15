package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type StreamEntry struct {
	ID		string
	Fields	[]string
}

type KeyValue struct {
	Value       string
	ExpiryTime  *time.Time
}

var streamData = make(map[string][]StreamEntry);
var setGetMap = make(map[string]string);
var expiryMap = make(map[string]time.Time)

func ParseData(data []byte, connection net.Conn, server *Server) {
	if data[0] == '$' {
		handleBulkStrings(data);
	} else if data[0] == '*' {
		handleArray(data, connection, server);
	}
}

func handleArray(data []byte, connection net.Conn, server *Server) {
	dataStr := string(data);
	parts := strings.Split(dataStr, "\r\n");
	parts = parts[:len(parts) - 1];
	if len(parts) == 1 && parts[0] == "*" {
		return;
	}
	fmt.Println("parts: ", parts);
	numberOfElements, _ := strconv.Atoi(strings.Split(parts[0], "*")[1]);
	actualNumberOfElements := (len(parts)) / 2;

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
			server.offset += 14;

			for _, conn := range server.otherServersConn {
				if conn == connection {
					// dont return pong but still add to the offset
					return;
				}
			}
			_, err := connection.Write([]byte("+PONG\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}
	} else {
		for i := 1; i < len(parts); i += 2 {
			wordLen, _ := strconv.Atoi(strings.Split(parts[i], "$")[1]);
			actualWordLen := len(parts[i+1]);
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
			
			server.offset += len(dataStr);
			
			key	:= parts[4];
			value := parts[6];

			setGetMap[key] = value;

			if len(parts) == 11 {
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
			

				if server.role == "master" {
					dataToSendSlave := "*3\r\n$3\r\nset\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n" + "$" + strconv.Itoa(len(parts[8])) + "\r\n" + parts[8] + "\r\n" + "$" + strconv.Itoa(len(parts[10])) + "\r\n" + parts[10] + "\r\n";
					
					for _, conn := range server.otherServersConn {
						conn.Write([]byte(dataToSendSlave));
					}
				}

			} else {
				if server.role == "master" {
					dataToSendSlave := "*3\r\n$3\r\nset\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n";
				
					for _, conn := range server.otherServersConn {
						conn.Write([]byte(dataToSendSlave));
					}
				}
			}

			for _, conn := range server.otherServersConn {
				if conn == connection {
					// dont return ok but still add to the offset
					return;
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
				filePath := Dir + "/" + Dbfilename;

				file, err := os.Open(filePath)
				if err != nil {
					fmt.Println("Error opening RDB file:", err)
					return
				}
				defer file.Close()

				file.Seek(9, 0)

				keyValueMap, err := readAllKeyValues(file)
				if err != nil {
					fmt.Println("Error reading key-value pairs from RDB file:", err)
					return
				}

				// set the values in the map, so that we dont have to read the file again for keys in the rdb file
				for key, value := range keyValueMap {
					if(value.ExpiryTime == nil || time.Now().Before(*value.ExpiryTime)) {
						setGetMap[key] = value.Value;
					}
				}

				value, ok := keyValueMap[parts[4]]
				if ok {
					if value.ExpiryTime != nil && time.Now().After(*value.ExpiryTime) {
						_, err := connection.Write([]byte("$-1\r\n"))
						if err != nil {
							fmt.Println("Error writing:", err.Error())
						}
						return
					}
					dataToSend := "$" + strconv.Itoa(len(value.Value)) + "\r\n" + value.Value + "\r\n"
					_, err := connection.Write([]byte(dataToSend))
					if err != nil {
						fmt.Println("Error writing:", err.Error())
					}
				} else {
					_, err := connection.Write([]byte("$-1\r\n"))
					if err != nil {
						fmt.Println("Error writing:", err.Error())
					}
				}
			}
		} else if strings.ToLower(parts[2]) == "info" {
			if strings.ToLower(parts[4]) == "replication" {
				role := "";
				replOffset := "0";

				if server.role == "slave" {
					role = "slave";
				} else {
					role = "master";
				}

				dataToSend := "role:" + role + "\r\n" +
							"master_replid:" + server.replId + "\r\n" +
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
			} else if strings.ToLower(parts[4]) == "getack" && strings.ToLower(parts[6]) == "*" {
				serverOffset := server.offset;
				respToSend := "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + strconv.Itoa(len(strconv.Itoa(serverOffset))) + "\r\n" + strconv.Itoa(serverOffset) + "\r\n"
				_, err := connection.Write([]byte(respToSend))
				if err != nil {
					fmt.Println("Error writing:", err.Error())
				}
				server.offset += len(dataStr);
			}
		} else if strings.ToLower(parts[2]) == "psync" && strings.ToLower(parts[4]) == "?" && strings.ToLower(parts[6]) == "-1" {
			
			dataToSend := "+FULLRESYNC " + server.replId + " 0\r\n";
			_, err := connection.Write([]byte(dataToSend));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}

			rdbHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
			rdbBytes, _ := hex.DecodeString(rdbHex);

			dataToSend = "$" + strconv.Itoa(len(rdbBytes)) + "\r\n" + string(rdbBytes);
			_, err2 := connection.Write([]byte(dataToSend));
			if err2 != nil {
				fmt.Println("Error writing:", err.Error());
			}
		} else if strings.ToLower(parts[2]) == "wait" {
			replicasToWaitFor := parts[4];

			if replicasToWaitFor == "0" {
				dataToSend := ":0\r\n";
				_, err := connection.Write([]byte(dataToSend));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
			}

			for _, conn := range server.otherServersConn {
				conn.Write([]byte("*3\r\n$8\r\nreplconf\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"));
			}

			timeToWait := parts[6];
			timeToWaitInt, _ := strconv.Atoi(timeToWait);
			time.Sleep(time.Duration(timeToWaitInt) * time.Millisecond);
			
			replicaCount := AckCount;
			if AckCount == 0 {
				replicaCount = len(server.otherServersConn);
			}
			dataToSend := ":" + strconv.Itoa(replicaCount) + "\r\n";
			_, err := connection.Write([]byte(dataToSend));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
			AckCount = 0;
		} else if strings.ToLower(parts[2]) == "config" {
			if strings.ToLower(parts[4]) == "get" {
				if strings.ToLower(parts[6]) == "dir" {
					dataToSend := "*2\r\n$3\r\ndir\r\n$" + strconv.Itoa(len(Dir)) + "\r\n" + Dir + "\r\n";
					_, err := connection.Write([]byte(dataToSend));
					if err != nil {
						fmt.Println("Error writing:", err.Error());
					}
				} else if strings.ToLower(parts[6]) == "dbfilename" {
					dataToSend := "*2\r\n$10\r\ndbfilename\r\n$" + strconv.Itoa(len(Dbfilename)) + "\r\n" + Dbfilename + "\r\n";
					_, err := connection.Write([]byte(dataToSend));
					if err != nil {
						fmt.Println("Error writing:", err.Error());
					}
				}
			}
		} else if strings.ToLower(parts[2]) == "type" {

			_, ok := streamData[parts[4]];
			if ok {
				_, err := connection.Write([]byte("+stream\r\n"));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
				return;
			}

			_, ok = setGetMap[parts[4]];
			if ok {
				expiry, ok := expiryMap[parts[4]];

				if ok && time.Now().After(expiry) {
					delete(setGetMap, parts[4]);
					delete(expiryMap, parts[4]);

					_, err := connection.Write([]byte("+none\r\n"));
					if err != nil {
						fmt.Println("Error writing:", err.Error());
					}	

					return;
				}

				_, err := connection.Write([]byte("+string\r\n"));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
				return;
			} else {
				_, err := connection.Write([]byte("+none\r\n"));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
				return;
			}
		} else if strings.ToLower(parts[2]) == "xadd" {
			streamKey := parts[4];
			streamKeysId := parts[6];

			if streamKeysId == "*" {
				
				keyValues := parts[7:];
				if len(keyValues) % 2 != 0 {
					fmt.Println("Error: Invalid number of key value pairs");
					return;
				}

				_, ok := streamData[streamKey];
				if !ok {
					milisec := time.Now().UnixNano() / int64(time.Millisecond);
					streamKeysId = strconv.Itoa(int(milisec)) + "-0";

					var keyValArr []string;
					for i := 0; i < len(keyValues); i += 4 {
						key := keyValues[i + 1];
						value := keyValues[i + 3];

						keyValArr = append(keyValArr, key);
						keyValArr = append(keyValArr, value);
					}

					streamData[streamKey] = append(streamData[streamKey], StreamEntry{
						ID: streamKeysId,
						Fields: keyValArr,
					});
				} else {
					highestMili := -1;
					highestMilisSeq := 0;
					
					for _, entry := range streamData[streamKey] {
						idParts := strings.Split(entry.ID, "-");
						mili, _ := strconv.Atoi(idParts[0]);
						seq, _ := strconv.Atoi(idParts[1]);

						if mili >= highestMili {
							highestMili = mili;
							highestMilisSeq = seq;
						}
					}

					milisec := time.Now().UnixNano() / int64(time.Millisecond);

					if milisec > int64(highestMili) {
						streamKeysId = strconv.Itoa(int(milisec)) + "-0";
					} else if milisec == int64(highestMili) {
						streamKeysId = strconv.Itoa(int(milisec)) + "-" + strconv.Itoa(highestMilisSeq + 1);
					} else {
						_, err := connection.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
						if err != nil {
							fmt.Println("Error writing:", err.Error());
						}
						return;
					}

					var keyValArr []string;
					for i := 0; i < len(keyValues); i += 4 {
						key := keyValues[i + 1];
						value := keyValues[i + 3];

						keyValArr = append(keyValArr, key);
						keyValArr = append(keyValArr, value);
					}
					streamData[streamKey] = append(streamData[streamKey], StreamEntry{
						ID: streamKeysId,
						Fields: keyValArr,
					});
				}

				dataToSend := "$" + strconv.Itoa(len(streamKeysId)) + "\r\n" + streamKeysId + "\r\n";
				_, err := connection.Write([]byte(dataToSend));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}

			} else {
				idParts := strings.Split(streamKeysId, "-");
				if idParts[1] == "*" {

					if len(idParts) != 2 {
						_, err := connection.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
						if err != nil {
							fmt.Println("Error writing:", err.Error());
						}
						return;
					}

					keyValues := parts[7:];
					if len(keyValues) % 2 != 0 {
						fmt.Println("Error: Invalid number of key value pairs");
						return;
					}

					_, ok := streamData[streamKey];
					if !ok {
						if idParts[0] == "0" {
							streamKeysId = "0-1";
						} else {
							streamKeysId = idParts[0] + "-0";
						}

						var keyValArr []string;
						for i := 0; i < len(keyValues); i += 4 {
							key := keyValues[i + 1];
							value := keyValues[i + 3];

							keyValArr = append(keyValArr, key);
							keyValArr = append(keyValArr, value);
						}

						streamData[streamKey] = append(streamData[streamKey], StreamEntry{
							ID: streamKeysId,
							Fields: keyValArr,
						})
					} else {
						highestMili := -1;
						highestMilisSeq := 0;

						for _, entry := range streamData[streamKey] {
							idParts := strings.Split(entry.ID, "-");
							mili, _ := strconv.Atoi(idParts[0]);
							seq, _ := strconv.Atoi(idParts[1]);

							if mili >= highestMili {
								highestMili = mili;
								highestMilisSeq = seq;
							}
						}

						idPart0, _ := strconv.Atoi(idParts[0]);

						if idPart0 > highestMili {
							streamKeysId = idParts[0] + "-0";
						} else if idPart0 == highestMili {
							streamKeysId = idParts[0] + "-" + strconv.Itoa(highestMilisSeq + 1);
						} else {
							_, err := connection.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
							if err != nil {
								fmt.Println("Error writing:", err.Error());
							}
							return;
						}

						var keyValArr []string;
						for i := 0; i < len(keyValues); i += 4 {
							key := keyValues[i + 1];
							value := keyValues[i + 3];

							keyValArr = append(keyValArr, key);
							keyValArr = append(keyValArr, value);
						}
						streamData[streamKey] = append(streamData[streamKey], StreamEntry{
							ID: streamKeysId,
							Fields: keyValArr,
						});
					}

					dataToSend := "$" + strconv.Itoa(len(streamKeysId)) + "\r\n" + streamKeysId + "\r\n";
					_, err := connection.Write([]byte(dataToSend));
					if err != nil {
						fmt.Println("Error writing:", err.Error());
					}

				} else {
					if len(idParts) != 2 {
						_, err := connection.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
						if err != nil {
							fmt.Println("Error writing:", err.Error());
						}
						return;
					} else if idParts[0] == "0" && idParts[1] == "0" {
						_, err := connection.Write([]byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"));
						if err != nil {
							fmt.Println("Error writing:", err.Error());
						}
						return;
					}

					keyValues := parts[7:];
					if len(keyValues) % 2 != 0 {
						fmt.Println("Error: Invalid number of key value pairs");
						return;
					}

					_, ok := streamData[streamKey];
					if !ok {
						var keyValArr []string;

						for i := 0; i < len(keyValues); i += 4 {
							key := keyValues[i + 1];
							value := keyValues[i + 3];

							keyValArr = append(keyValArr, key);
							keyValArr = append(keyValArr, value);
						}

						streamData[streamKey] = append(streamData[streamKey], StreamEntry{
							ID: streamKeysId,
							Fields: keyValArr,
						});
					} else {
						highestMili := -1;
						highestSeq := -1;

						for _, entry := range streamData[streamKey] {
							idParts := strings.Split(entry.ID, "-");
							mili, _ := strconv.Atoi(idParts[0]);
							seq, _ := strconv.Atoi(idParts[1]);

							if mili > highestMili {
								highestMili = mili;
								highestSeq = seq;
							} else if mili == highestMili && seq > highestSeq {
								highestSeq = seq;
							}
						}

						idPart0, _ := strconv.Atoi(idParts[0])
						idPart1, _ := strconv.Atoi(idParts[1])
						if idPart0 >= highestMili {
							if idPart1 <= highestSeq {
								_, err := connection.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
								if err != nil {
									fmt.Println("Error writing:", err.Error());
								}
								return;
							}
						} else {
							_, err := connection.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
							if err != nil {
								fmt.Println("Error writing:", err.Error());
							}
							return;
						}

						var keyValArr []string;
						for i := 0; i < len(keyValues); i += 4 {
							key := keyValues[i + 1];
							value := keyValues[i + 3];

							keyValArr = append(keyValArr, key);
							keyValArr = append(keyValArr, value);
						}
						streamData[streamKey] = append(streamData[streamKey], StreamEntry{
							ID: streamKeysId,
							Fields: keyValArr,
						})
					}
					dataToSend := "$" + strconv.Itoa(len(streamKeysId)) + "\r\n" + streamKeysId + "\r\n";
					_, err := connection.Write([]byte(dataToSend));
					if err != nil {
						fmt.Println("Error writing:", err.Error());
					}
				}
			}
		} else if strings.ToLower(parts[2]) == "xrange" {
			streamKey := parts[4];
			
			_, ok := streamData[streamKey];
			if !ok {
				_, err := connection.Write([]byte("*0\r\n"));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
				return;
			}

			if len(parts) > 5 {
				if parts[8] == "+" {
					start := parts[6];

					matchingEntries := []StreamEntry{}
					for _, entry := range streamData[streamKey] {
						if entry.ID >= start {
							matchingEntries = append(matchingEntries, entry);
						}
					}

					dataToSend := "*" + strconv.Itoa(len(matchingEntries)) + "\r\n";
					for _, entry := range matchingEntries {
						dataToSend += "*2\r\n$" + strconv.Itoa(len(entry.ID)) + "\r\n" + entry.ID + "\r\n*" + strconv.Itoa(len(entry.Fields)) + "\r\n";
						for i := 0; i < len(entry.Fields); i += 2 {
							dataToSend += "$" + strconv.Itoa(len(entry.Fields[i])) + "\r\n" + entry.Fields[i] + "\r\n" + "$" + strconv.Itoa(len(entry.Fields[i + 1])) + "\r\n" + entry.Fields[i + 1] + "\r\n";
						}
					}

					_, err := connection.Write([]byte(dataToSend));
					if err != nil {
						fmt.Println("Error writing:", err.Error());
					}
				} else {
					start := parts[6]
					end := parts[8]
				
					matchingEntries := []StreamEntry{}
					for _, entry := range streamData[streamKey] {
						if entry.ID >= start && entry.ID <= end {
							matchingEntries = append(matchingEntries, entry)
						}
					}
				
					dataToSend := "*" + strconv.Itoa(len(matchingEntries)) + "\r\n"
					for _, entry := range matchingEntries {
						dataToSend += "*2\r\n$" + strconv.Itoa(len(entry.ID)) + "\r\n" + entry.ID + "\r\n*" + strconv.Itoa(len(entry.Fields)) + "\r\n"
						for i := 0; i < len(entry.Fields); i += 2 {
							dataToSend += "$" + strconv.Itoa(len(entry.Fields[i])) + "\r\n" + entry.Fields[i] + "\r\n" + "$" + strconv.Itoa(len(entry.Fields[i + 1])) + "\r\n" + entry.Fields[i + 1] + "\r\n"
						}
					}
				
					_, err := connection.Write([]byte(dataToSend))
					if err != nil {
						fmt.Println("Error writing:", err.Error())
					}
				}
			} else {
				dataToSend := "*" + strconv.Itoa(len(streamData[streamKey])) + "\r\n";
				
				for _, entry := range streamData[streamKey] {
					dataToSend += "*2\r\n$" + strconv.Itoa(len(entry.ID)) + "\r\n" + entry.ID + "\r\n*" + strconv.Itoa(len(entry.Fields)) + "\r\n";
					for i := 0; i < len(entry.Fields); i += 2 {
						dataToSend += "$" + strconv.Itoa(len(entry.Fields[i])) + "\r\n" + entry.Fields[i] + "\r\n" + "$" + strconv.Itoa(len(entry.Fields[i + 1])) + "\r\n" + entry.Fields[i + 1] + "\r\n";
					}
				}

				_, err := connection.Write([]byte(dataToSend));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
			}
		} else if strings.ToLower(parts[2]) == "xread" {
			if strings.ToLower(parts[4]) == "streams" {
				dataToSend := ""
				streamCount := 0
				for i := 6; i < len(parts); i += 2 {
					streamKey := parts[i]
					startExclusive := "0-0"
					if i+1 < len(parts) {
						startExclusive = parts[i+1]
					}
		
					_, ok := streamData[streamKey]
					if !ok {
						continue
					}
		
					matchingEntries := []StreamEntry{}
					for _, entry := range streamData[streamKey] {
						if entry.ID > startExclusive {
							matchingEntries = append(matchingEntries, entry)
						}
					}
		
					if len(matchingEntries) > 0 {
						streamCount++
						dataToSend += "*2\r\n$" + strconv.Itoa(len(streamKey)) + "\r\n" + streamKey + "\r\n*" + strconv.Itoa(len(matchingEntries)) + "\r\n"
						for _, entry := range matchingEntries {
							dataToSend += "*2\r\n$" + strconv.Itoa(len(entry.ID)) + "\r\n" + entry.ID + "\r\n*" + strconv.Itoa(len(entry.Fields)) + "\r\n"
							for i := 0; i < len(entry.Fields); i += 2 {
								dataToSend += "$" + strconv.Itoa(len(entry.Fields[i])) + "\r\n" + entry.Fields[i] + "\r\n" + "$" + strconv.Itoa(len(entry.Fields[i + 1])) + "\r\n" + entry.Fields[i + 1] + "\r\n"
							}
						}
					}
				}
		
				if streamCount > 0 {
					dataToSend = "*" + strconv.Itoa(streamCount) + "\r\n" + dataToSend
					_, err := connection.Write([]byte(dataToSend))
					if err != nil {
						fmt.Println("Error writing:", err.Error())
					}
				} else {
					_, err := connection.Write([]byte("*-1\r\n"))
					if err != nil {
						fmt.Println("Error writing:", err.Error())
					}
				}
			} else if strings.ToLower(parts[4]) == "block" && strings.ToLower(parts[8]) == "streams" {
				dataToSend := ""
				streamCount := 0;
				blockTimeInMilli, _ := strconv.Atoi(parts[6]);
				streamKey := parts[10];
				streamId := parts[12];

				if streamId == "$" {
					maxId := "0"
					for _, entry := range streamData[streamKey] {
						if entry.ID > maxId {
							maxId = entry.ID
						}
					}
					streamId = maxId
				}

				if blockTimeInMilli != 0 {
					time.Sleep(time.Duration(blockTimeInMilli) * time.Millisecond);
				}

				_, ok := streamData[streamKey];
				if !ok {
					_, err := connection.Write([]byte("*-1\r\n"));
					if err != nil {
						fmt.Println("Error writing:", err.Error());
					}
					return;
				}

				matchingEntries := []StreamEntry{};
				for _, entry := range streamData[streamKey] {
					if entry.ID > streamId {
						matchingEntries = append(matchingEntries, entry);
					}
				}
				if len(matchingEntries) > 0 {
					streamCount++;
					dataToSend += "*2\r\n$" + strconv.Itoa(len(streamKey)) + "\r\n" + streamKey + "\r\n*" + strconv.Itoa(len(matchingEntries)) + "\r\n"
					for _, entry := range matchingEntries {
						dataToSend += "*2\r\n$" + strconv.Itoa(len(entry.ID)) + "\r\n" + entry.ID + "\r\n*" + strconv.Itoa(len(entry.Fields)) + "\r\n"
						for i := 0; i < len(entry.Fields); i += 2 {
							dataToSend += "$" + strconv.Itoa(len(entry.Fields[i])) + "\r\n" + entry.Fields[i] + "\r\n" + "$" + strconv.Itoa(len(entry.Fields[i + 1])) + "\r\n" + entry.Fields[i + 1] + "\r\n"
						}
					}
				} else if len(matchingEntries) == 0 && blockTimeInMilli == 0 {
					x := len(streamData);
					for {
						streamMapLen := len(streamData);
						if streamMapLen >= x {
							matchingEntries := []StreamEntry{};
							for _, entry := range streamData[streamKey] {
								if entry.ID > streamId {
									matchingEntries = append(matchingEntries, entry);
									break;
								}
							}
							if len(matchingEntries) > 0 {
								streamCount++;
								dataToSend += "*2\r\n$" + strconv.Itoa(len(streamKey)) + "\r\n" + streamKey + "\r\n*" + strconv.Itoa(len(matchingEntries)) + "\r\n"
								for _, entry := range matchingEntries {
									dataToSend += "*2\r\n$" + strconv.Itoa(len(entry.ID)) + "\r\n" + entry.ID + "\r\n*" + strconv.Itoa(len(entry.Fields)) + "\r\n"
									for i := 0; i < len(entry.Fields); i += 2 {
										dataToSend += "$" + strconv.Itoa(len(entry.Fields[i])) + "\r\n" + entry.Fields[i] + "\r\n" + "$" + strconv.Itoa(len(entry.Fields[i + 1])) + "\r\n" + entry.Fields[i + 1] + "\r\n"
									}
								}
								break;
							}
						}
						time.Sleep(100 * time.Millisecond);
					}
				}

				if streamCount > 0 {
					dataToSend = "*" + strconv.Itoa(streamCount) + "\r\n" + dataToSend
					_, err := connection.Write([]byte(dataToSend))
					if err != nil {
						fmt.Println("Error writing:", err.Error())
					}
				} else {
					_, err := connection.Write([]byte("*-1\r\n"))
					if err != nil {
						fmt.Println("Error writing:", err.Error())
					}
				}
			}
		} else if strings.ToLower(parts[2]) == "keys" {
			if strings.ToLower(parts[4]) == "*" {
				filePath := Dir + "/" + Dbfilename;

				file, err := os.Open(filePath)
				if err != nil {
					fmt.Println("Error opening RDB file:", err)
					return
				}
				defer file.Close()

				file.Seek(9, 0)

				keyValueMap, err := readAllKeyValues(file)
				if err != nil {
					fmt.Println("Error reading key-value pairs from RDB file:", err)
					return
				}

				availableKeysArray := []string{};
				for key, value := range keyValueMap {
					if(value.ExpiryTime == nil || time.Now().Before(*value.ExpiryTime)) {
						availableKeysArray = append(availableKeysArray, key);
					}	
				}

				dataToSend := "*" + strconv.Itoa(len(availableKeysArray)) + "\r\n";
				for _, key := range availableKeysArray {
					dataToSend += "$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n";
				}

				_, err = connection.Write([]byte(dataToSend));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
			}
		}
    }
}

func handleBulkStrings(date []byte) {
}

// -------------------------------------------------------------------
// @Utility for "KEYS *"

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
