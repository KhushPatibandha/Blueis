package cmd

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func HandleXadd(connection net.Conn, parts []string, streamData map[string][]typestructs.StreamEntry) {

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

			streamData[streamKey] = append(streamData[streamKey], typestructs.StreamEntry{
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
			streamData[streamKey] = append(streamData[streamKey], typestructs.StreamEntry{
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

				streamData[streamKey] = append(streamData[streamKey], typestructs.StreamEntry{
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
				streamData[streamKey] = append(streamData[streamKey], typestructs.StreamEntry{
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

				streamData[streamKey] = append(streamData[streamKey], typestructs.StreamEntry{
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
				streamData[streamKey] = append(streamData[streamKey], typestructs.StreamEntry{
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
}