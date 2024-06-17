package cmd

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	rdbutil "github.com/codecrafters-io/redis-starter-go/app/rdbUtil"
)

func HandleKeys(connection net.Conn, parts []string, dir string, dbfilename string) {
	if strings.ToLower(parts[4]) == "*" {
		filePath := dir + "/" + dbfilename;

		file, err := os.Open(filePath)
		if err != nil {
			fmt.Println("Error opening RDB file:", err)
			return
		}
		defer file.Close()

		file.Seek(9, 0)

		keyValueMap, err := rdbutil.ReadAllKeyValues(file)
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