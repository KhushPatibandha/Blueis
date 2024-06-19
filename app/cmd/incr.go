package cmd

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	rdbutil "github.com/codecrafters-io/redis-starter-go/app/rdbUtil"
	typestructs "github.com/codecrafters-io/redis-starter-go/typeStructs"
)

func HandleIncr(connection net.Conn, server *typestructs.Server, parts []string, setGetMap map[string]string, expiryMap map[string]time.Time, dataStr string, dir string, dbfilename string) {
	server.Offset += len(dataStr);
			
	key	:= parts[4];

	keyToGet, ok := setGetMap[key];

	if ok {
		expiry, ok := expiryMap[key];

		if ok && time.Now().After(expiry) {
			delete(setGetMap, key);
			delete(expiryMap, key);

			// insted of returning -1, add the key to setGetMap with value 1 and return that

			setGetMap[key] = "1";

			_, err := connection.Write([]byte(":1\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}

			return;
		}

		incrData, err := strconv.Atoi(keyToGet);
		if err != nil {
			fmt.Println("Error converting key to int.");
		}
		incrData += 1;

		dataToSend := ":" + strconv.Itoa(incrData) + "\r\n";
		_, err = connection.Write([]byte(dataToSend));
		if err != nil {
			fmt.Println("Error writing:", err.Error());
		}

		return
	} else {
		filePath := dir + "/" + dbfilename;

		if filePath != "/" {
			file, err := os.Open(filePath)
			if err != nil {
				fmt.Println("Error opening RDB file:", err)
				return
			}
			defer file.Close()

			file.Seek(9, 0)

			keyValueMap, err := rdbutil.ReadAllKeyValues(file);
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

			value, ok := keyValueMap[key];

			if ok {
				if value.ExpiryTime != nil && time.Now().After(*value.ExpiryTime) {
					setGetMap[key] = "1";
					
					_, err := connection.Write([]byte(":1\r\n"));
					if err != nil {
						fmt.Println("Error writing:", err.Error())
					}
					return
				}
				incrData, err := strconv.Atoi(value.Value);
				if err != nil {
					fmt.Println("Error converting key to int.");
				}
				incrData += 1;

				dataToSend := ":" + strconv.Itoa(incrData) + "\r\n";
				_, err = connection.Write([]byte(dataToSend));
				if err != nil {
					fmt.Println("Error writing:", err.Error());
				}
			} else {
				setGetMap[key] = "1";
					
				_, err := connection.Write([]byte(":1\r\n"));
				if err != nil {
					fmt.Println("Error writing:", err.Error())
				}
				return
			}

		} else {

			// If key was not in the set previously, add it to the set with value 1

			setGetMap[key] = "1";

			_, err := connection.Write([]byte(":1\r\n"));
			if err != nil {
				fmt.Println("Error writing:", err.Error());
			}

			return;
		}
	}
}