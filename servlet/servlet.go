package servlet

import (
	"github.com/tigeress/parameterserver/protos"
	"net"
)
type Servlet struct{
	Conn net.Conn
	Command *protos.Command
	Response *protos.Response
	PartitionKey string
}
