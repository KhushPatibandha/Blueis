package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()));
var role = "master";
var replId = getHash(40);

func main() {
    port := flag.String("port", "6379", "port to listen on");
	replicaof := flag.String("replicaof", "", "master server to replicate from");
    flag.Parse();

	if *replicaof != "" {
		role = "slave";
	}

    l, err := net.Listen("tcp", "0.0.0.0:"+*port)
    if err != nil {
        fmt.Println("Failed to bind to port", *port)
        os.Exit(1)
    }
    defer l.Close()

    for {
        connection, err := l.Accept()
        if err != nil {
            fmt.Println("Error accepting connection: ", err.Error())
            os.Exit(1)
        }
        go handleConnection(connection)
    }
}

func handleConnection(connection net.Conn) {
    defer connection.Close()
    for {
        buf := make([]byte, 1024)
        _, err := connection.Read(buf)
        if err != nil {
            if err == io.EOF {
                return
            }
            fmt.Println("Error reading:", err.Error())
        }
        ParseData(buf, connection)
    }
}

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

func GetRole() string {
	return role;
}

func GetReplId() string {
	return replId;
}
