package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()));
var role = "master";
var replId = getHash(40);

func main() {
    port := flag.String("port", "6379", "port to listen on")
    replicaof := flag.String("replicaof", "", "master server to replicate from")
    flag.Parse()

    if *replicaof != "" {
        role = "slave"
        masterHost, masterPort := splitHostPort(*replicaof)
        connectToMaster(masterHost + ":" + masterPort, *port);
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

func connectToMaster(master string, port string) {
    conn, err := net.Dial("tcp", master)
    if err != nil {
        fmt.Println("Failed to connect to master", master)
        fmt.Println("Error:", err);
		os.Exit(1)
    }
	defer conn.Close();
	buf := make([]byte, 1024)

    fmt.Fprintf(conn, "*1\r\n$4\r\nPING\r\n")
    _, err = conn.Read(buf)
    if err != nil {
        fmt.Println("Error reading:", err.Error())
        os.Exit(1)
    }

	fmt.Fprintf(conn, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(port), port)
    _, err = conn.Read(buf)
    if err != nil {
        fmt.Println("Error reading:", err.Error())
        os.Exit(1)
    }

	fmt.Fprintf(conn, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
    _, err = conn.Read(buf)
    if err != nil {
        fmt.Println("Error reading:", err.Error())
        os.Exit(1)
    }

	fmt.Fprintf(conn, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
	_, err = conn.Read(buf)
    if err != nil {
        fmt.Println("Error reading:", err.Error())
        os.Exit(1)
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

func splitHostPort(replicaof string) (string, string) {
    parts := strings.Split(replicaof, " ")
    return parts[0], parts[1]
}
