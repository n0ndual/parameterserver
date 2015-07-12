package servlet

import (
	"github.com/scorpionis/parameterserver/protos"
	"net"
)
type Servlet struct{
	Conn net.Conn
	Command *protos.Command
	Response *protos.Response
	PartitionKey string
}
