package cmd

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	rdbutil "github.com/codecrafters-io/redis-starter-go/app/rdbUtil"
)

func HandleGet(connection net.Conn, parts []string, setGetMap map[string]string, expiryMap map[string]time.Time, dataStr string, dir string, dbfilename string, flag bool) string {
	
	if flag {
		_, ok := connAndCommands[connection];
		if ok {
			connAndCommands[connection] = append(connAndCommands[connection], dataStr);
			
			_, err := connection.Write([]byte("+QUEUED\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
			return "+QUEUED\r\n";
		}
	}
	
	keyToGet, ok := setGetMap[parts[4]];
	if ok {
		expiry, ok := expiryMap[parts[4]];

		if ok && time.Now().After(expiry) {
			delete(setGetMap, parts[4]);
			delete(expiryMap, parts[4]);

			if flag {
				_, err := connection.Write([]byte("$-1\r\n"));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
			}	

			return "$-1\r\n";
		}

		dataToSend := "$" + strconv.Itoa(len(keyToGet)) + "\r\n" + keyToGet + "\r\n";

		if flag {
			_, err := connection.Write([]byte(dataToSend));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}
		}

		return dataToSend;
	} else {
		filePath := dir + "/" + dbfilename;

		if filePath != "/" {
			
			file, err := os.Open(filePath)
			if err != nil {
				fmt.Println("Error opening RDB file:", err)
				return "null";
			}
			defer file.Close()

			file.Seek(9, 0)

			keyValueMap, err := rdbutil.ReadAllKeyValues(file);
			if err != nil {
				fmt.Println("Error reading key-value pairs from RDB file:", err)
				return "null";
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
					
					if flag {
						_, err := connection.Write([]byte("$-1\r\n"))
						if err != nil {
							fmt.Println("Error writing:", err.Error())
						}
					}

					return "$-1\r\n";
				}

				dataToSend := "$" + strconv.Itoa(len(value.Value)) + "\r\n" + value.Value + "\r\n"
				
				if flag {
					_, err := connection.Write([]byte(dataToSend))
					if err != nil {
						fmt.Println("Error writing:", err.Error())
					}
				}
				return dataToSend;
			} else {
				if flag {
					_, err := connection.Write([]byte("$-1\r\n"))
					if err != nil {
						fmt.Println("Error writing:", err.Error())
					}
				}

				return "$-1\r\n";
			}
		} else {
			if flag {
				_, err := connection.Write([]byte("$-1\r\n"))
				if err != nil {
					fmt.Println("Error writing:", err.Error())
				}
			}

			return "$-1\r\n";
		}
	}
}