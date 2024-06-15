package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Server struct {
    role                string
    port                int
    replId              string
    offset              int
    otherServersConn    []net.Conn
}

var AckCount = 0;
var masterPortGlobal int;
const charset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
var Dir = "";
var Dbfilename = "";

func main() {
    // .spwan_redis_server.sh --port 6380 --replicaof "localhost 6379"

    var wg sync.WaitGroup;
    
    port := flag.String("port", "6379", "port to listen on");
    replicaof := flag.String("replicaof", "", "master server to replicate from");
    dir := flag.String("dir", "", "directory to save data");
    dbfilename := flag.String("dbfilename", "", "filename to save data");
    flag.Parse();

    Dir = *dir;
    Dbfilename = *dbfilename;

    portInt, _ := strconv.Atoi(*port);

    if *port != "" && *replicaof != "" {
        _, masterPort := splitHostPort(*replicaof);
        masterPortInt, _ := strconv.Atoi(masterPort);
        masterPortGlobal = masterPortInt;

        conn, err := net.Listen("tcp", "localhost:"+masterPort);
        if err != nil {
            fmt.Println("Master server is already running on port: ", masterPort);
        } else {
            conn.Close();
            masterReplId := getHash(40);
            masterServer := Server{role: "master", port: masterPortInt, replId: masterReplId, offset: 0}
            wg.Add(1);
            go func()  {
                spawnServer(&masterServer);
                wg.Done();
            }();
        }

        slaveReplId := getHash(40);
        slaveServer := Server{role: "slave", port: portInt, replId: slaveReplId, offset: 0}

        wg.Add(1);
        go func() {
            spawnServer(&slaveServer);
            wg.Done();
        }();
    } else if *port != "" && *replicaof == "" {
        masterReplId := getHash(40);
        masterServer := Server{role: "master", port: portInt, replId: masterReplId, offset: 0}

        wg.Add(1);
        go func() {
            spawnServer(&masterServer);
            wg.Done();
        }()
        
    } else {
        masterReplId := getHash(40);
        masterServer := Server{role: "master", port: portInt, replId: masterReplId, offset: 0}
        
        wg.Add(1);
        go func() {
            spawnServer(&masterServer);
            wg.Done();
        }()
    }
    wg.Wait();
}

func spawnServer(server *Server) {
    l, err := net.Listen("tcp", "localhost:"+strconv.Itoa(server.port));
    if err != nil {
        fmt.Println("Failed to bind to port: ", server.port);
        fmt.Println("Error:", err);
        os.Exit(1);
    }

    if server.role == "slave" {
        masterConn := connectToMaster(server);
        performHandShake(masterConn, server);
        go handleConnection(masterConn, server);
    }

    for {
        conn, err := l.Accept();
        if err != nil {
            fmt.Println("Error accepting connection: ", err.Error());
            os.Exit(1);
        }
        go handleConnection(conn, server);
    }
}

func handleConnection(conn net.Conn, server *Server) {
    for {
        buf := make([]byte, 1024);

        bytesRead, err := conn.Read(buf);
        if err != nil {
            if err == io.EOF {
                return;
            }
            fmt.Println("Error reading:", err.Error());
        }

        data := buf[:bytesRead];
        if strings.Contains(string(data), "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n") {
            server.otherServersConn = append(server.otherServersConn, conn);
        } else if strings.Contains(string(data), "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$") {
            AckCount++;
        }

        command := strings.Split(string(data), "*");

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
                    ParseData([]byte(command[i]), conn, server)
                    i++;
                    continue;
                }
            } else {
                command[i] = "*" + command[i]
            }
            ParseData([]byte(command[i]), conn, server)
        }
            
    }
}

func connectToMaster(server *Server) net.Conn {
    conn, err := net.Dial("tcp", "localhost:" + strconv.Itoa(masterPortGlobal));
    fmt.Println("Connected to master server: ", conn.RemoteAddr(), &conn);

    if err != nil {
        fmt.Println("Failed to connect to master", masterPortGlobal);
        fmt.Println("Error:", err);
        return nil;
    }

    server.otherServersConn = append(server.otherServersConn, conn);

    return conn;
}

func performHandShake(conn net.Conn, server *Server) {
    slaveServerPort := server.port;
    slaveServerPortLen := len(strconv.Itoa(slaveServerPort));
    
    buf := make([]byte, 1024);

    var handShakeDataArray []string = []string{
        "*1\r\n$4\r\nPING\r\n",
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + strconv.Itoa(slaveServerPortLen) + "\r\n" + strconv.Itoa(slaveServerPort) + "\r\n",
        "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
        "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
    };
    
    for _, data := range handShakeDataArray {
        _, err := conn.Write([]byte(data));
        if err != nil {
            fmt.Println("Error writing:", err.Error());
        }

        _, err = conn.Read(buf);
        if err != nil {
            fmt.Println("Error reading:", err.Error());
        }
    }
}

func splitHostPort(replicaof string) (string, string) {
    parts := strings.Split(replicaof, " ")
    return parts[0], parts[1]
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