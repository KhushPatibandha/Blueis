package cmd

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleXread(connection net.Conn, parts []string, streamData map[string][]typestructs.StreamEntry) {
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

			matchingEntries := []typestructs.StreamEntry{}
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

		matchingEntries := []typestructs.StreamEntry{};
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
					matchingEntries := []typestructs.StreamEntry{};
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
}