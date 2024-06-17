package typestructs

import "net"

type Server struct {
    Role                string
    Port                int
    ReplId              string
    Offset              int
    OtherServersConn    []net.Conn
}