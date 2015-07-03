package command

import (
	"strconv"
	"strings"
	//"fmt"
	"bufio"
	"net"
	"io"
)

type Command struct{
	String string
	StringRedis string
	Argc int
	Args []string
	PartitionKey string
}
func EncodeRedisProtocol(cmd Command) string{
	var result []string
	result=append(result,"*"+strconv.Itoa(cmd.Argc)+"\r\n")
	for i:=0;i<cmd.Argc;i++ {
		result=append(result,"$"+strconv.Itoa(len(cmd.Args[i]))+"\r\n")
		result=append(result,cmd.Args[i]+"\r\n")
	}
	//fmt.Println(strings.Join(result,""))
	return string(strings.Join(result,""))
}
func DecodeRedisProtocol(conn net.Conn) *Command{
	cmd:=Command{}
	bufferedReader:=bufio.NewReader(conn)
	_,err:=bufferedReader.ReadString('*')
	if(err == io.EOF){
		conn.Close()
		return nil
	}
	argc,_:=bufferedReader.ReadString('\r')
	cmd.Argc,_=strconv.Atoi(strings.Trim(argc,"\r"))
	cmd.Args=make([]string,cmd.Argc)
	bufferedReader.ReadString('\n')
	for i:=0;i < cmd.Argc;i++ {
		bufferedReader.ReadString('\n')
		argu,_:=bufferedReader.ReadString('\r')
		cmd.Args[i]=strings.Trim(argu,"\r")
//		fmt.Println("argu: "+argu)
		bufferedReader.ReadString('\n')
	}
	return &cmd
}

func EncodeText(cmd Command) string{
	return strings.Join(cmd.Args," ")
}
func DecodeText(text string) Command{
	cmd:=Command{}
	cmd.String=text
	a1:=strings.Split(cmd.String," ")
	cmd.Argc=len(a1)
	cmd.Args=make([]string,cmd.Argc)
	for i:=0;i < cmd.Argc;i++ {
		cmd.Args[i]=a1[i]
	}
	return cmd
}
