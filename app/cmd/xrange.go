package cmd

import (
	"fmt"
	"net"
	"strconv"

	typestructs "github.com/codecrafters-io/redis-starter-go/typeStructs"
)

func HandleXrange(connection net.Conn, parts []string, streamData map[string][]typestructs.StreamEntry) {
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

			matchingEntries := []typestructs.StreamEntry{}
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
		
			matchingEntries := []typestructs.StreamEntry{}
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
}