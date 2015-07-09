package main

import (
	"io"
	"net"
	"fmt"
	"time"
	"stathat.com/c/consistent"
	"github.com/tigeress/goredis/command"
	"os"
	"github.com/tigeress/goredis/client"
	"github.com/golang/protobuf/proto"
	"github.com/tigeress/goredis/protos"
	"flag"
	"bufio"
	"strings"
	"strconv"
	"github.com/tigeress/goredis/servlet"
)

//把自己注册到dht中
var c  =consistent.New()
var servers map[string]string
var kv map[string]string
var keys map[string]int
var hostname string
var conns map[string]net.Conn
var server string
var ip string
var port string

var requestChannel chan *servlet.Servlet
var responseChannel chan *servlet.Servlet
func Init(){
	//flags
	flag.StringVar(&ip,"ip","localhost","server ip address")
	flag.StringVar(&port,"port",":7777","server tcp listen port")
	flag.Parse()
	server = ip+port
	fmt.Println(server)
	//command
	//fmt.Println(os.Args)
}
func main(){
	Init()
//	hostname,_ =os.Hostname()
//	servers=make(map[string]string)
//	servers["clive"]=ip
//	servers["clive0"]="7.7.7.1"
//	servers["clive1"]="7.7.7.7"
//	servers["clive2"]="7.7.7.9"
//	for k,_ :=range servers{
//		c.Add(k)
	//	}
	client.ConsitentHash=consistent.New()
	client.ConsistentHash.Add("7.7.7.1")
	client.ConsistentHash.Add("7.7.7.7")
	conn1,_:=net.Dial("tcp","7.7.7.1")
	conn2,_:=net.Dial("tcp","7.7.7.1")
	conn3,_:=net.Dial("tcp",ip)
	client.Conns["7.7.7.1"]=conn1
	client.Conns["7.7.7.7"]=conn2
	client.Conns["localhost"]=conn3
	fmt.Println(flag.Args())
	if flag.Args()[1]=="server" {
		if flag.Args()[0]=="start" {
			StartServer()
		}else if flag.Args()[0]=="stop" {
			//quit <- 0
		}
	}
	if flag.Args()[1]=="client" {
		fmt.Print("life is short, i use go -.-\n>")
		conn,_ :=net.Dial("tcp",server)
		for {
			line,_,_:=bufio.NewReader(os.Stdin).ReadLine()
			conn.Write([]byte(command.EncodeRedisProtocol(command.DecodeText(string(line)))))
			response,_:=bufio.NewReader(conn).ReadBytes('\n')
			fmt.Println(response)
			fmt.Print(">")
		}
	}
	//client.TestClient()
	//load Graph, why so slow
	if flag.Args()[1]=="loadGraph" {
		time.Sleep(100*time.Millisecond)
		fmt.Println("loadGraph start: "+time.Now().Format("2006-01-02 15:04:05"))
		client.LoadWebGraph()
		fmt.Println("loadGraph end: "+time.Now().Format("2006-01-02 15:04:05"))
	}
	if flag.Args()[1]=="pageRank" {
		fmt.Println("pagerank start: "+time.Now().Format("2006-01-02 15:04:05"))
		client.PageRank()
		fmt.Println("pagerank end: "+time.Now().Format("2006-01-02 15:04:05"))
	}
	if flag.Args()[1]=="test" {
		client.TestClientProto()
	}
}

func StartServer(){
	go KVStore()
	go ResponseThread()
	requestChannel=make(chan *servlet.Servlet)
	responseChannel=make(chan *servlet.Servlet)
	kv = make(map[string]string)
	keys=make(map[string]int)
	ln,err:=net.Listen("tcp",server)
	if err!=nil {
		fmt.Println(err)
	}
	fmt.Println("listen on tcp "+server)
	for {
		conn,err:=ln.Accept()
		if err!=nil {
			fmt.Println(err)
		}
		go ServletThread(conn)
	}

}
func ServletThread(conn net.Conn){
	for {
		err:=Server(conn)
		if err!=nil {
			return
		}
	}
}
func KVStore(){
	var request *servlet.Servlet
	for {
		select {
		case request= <- requestChannel:
			//fmt.Println("receive:"+request.Command.String())
			Execute(request)
		}
	}
}
func ResponseThread(){
	var response *servlet.Servlet
	for{
		select {
		case response= <- responseChannel:
			DoResponse(response)
		}
	}
}
func DoResponse(response *servlet.Servlet){
	//fmt.Println("to write:"+response.Response.String())
	responseBytes,_:=proto.Marshal(response.Response)
	response.Conn.Write(responseBytes)
}
func Execute(req *servlet.Servlet){
	request:=req.Command
	if (request.Type.String()=="Get"){
		value:=[]string{kv[*request.Key]}
		response:=protos.Response{
			Status:proto.Bool(true),
			Value: value,
		}
		req.Response=&response
		responseChannel <- req
	}
	if(request.Type.String()=="Set"){
		partitionKey:=req.PartitionKey
		keys[partitionKey]=1;
		kv[*request.Key]=*request.Value
		response:=protos.Response{
			Status:proto.Bool(true),
		}
		req.Response=&response
		responseChannel <- req
	}
	if(request.Type.String()=="Add"){
		partitionKey:=req.PartitionKey
		keys[partitionKey]=1;
		if kv[*request.Key]!=""&&kv[*request.Key]!="0"{
			oper1,_:=strconv.ParseFloat(kv[*request.Key],32)
			oper2,_:=strconv.ParseFloat(*request.Value,32)
			sum:=oper1+oper2
			kv[*request.Key]=strconv.FormatFloat(sum,'f',-1,32)
		}else{
			kv[*request.Key]=*request.Value
		}
		response:=protos.Response{
			Status:proto.Bool(true),
		}
		req.Response=&response
		responseChannel <- req
	}
	if(request.Type.String()=="Iterate"){
		//一次取出所有的key吗？只取partitionKey
		response:=protos.Response{
			Status:proto.Bool(true),
		}
		var iterate []string
		for k,_:=range keys{
//			fmt.Println("key: "+k)
			iterate=append(iterate,k)
		}
		response.Value=iterate
		//fmt.Println("result:"+result)
		req.Response=&response
		responseChannel <- req
	}
	if(request.Type.String()=="Flush"){
		if kv["Flush"]!=""&&kv["Flush"]!="0"{
			sum,_:=strconv.Atoi(kv["Flush"])
			kv["Flush"]=strconv.Itoa(sum+1)
		}else{
			kv["Flush"]="1"
		}
		if kv["Flush"]==strconv.Itoa(len(servers)) {
			Accumulator()
			kv["Flush"]="0"
		}
		response:=protos.Response{
			Status:proto.Bool(true),
		}
		req.Response=&response
		responseChannel <- req
	}
}
func Accumulator(){
	for k,_:=range keys{
		preRank,_:=strconv.ParseFloat(kv[k+".rank"],32)
		gradient,_:=strconv.ParseFloat(kv[k+".gradient"],32)
		newRank:=0.15*preRank+0.85*gradient
		kv[k+".rank"]=strconv.FormatFloat(newRank,'f',-1,32)
	}
}
func Server(conn net.Conn) error{
	var buf []byte
	data := make([]byte, 4096)
	n,err:= conn.Read(data)
	buf=append(buf,data[:n]...)
	for ;n==4096; {
		n,err=conn.Read(data)
		buf=append(buf,data[:n]...)
	}
//	data:=make([]byte,4096)
//	n,err:=conn.Read(data)
	if(err==io.EOF){
		//fmt.Println("close conn")
		//conn.Close()
		return err
	}
	servlet:=new(servlet.Servlet)
	servlet.Conn=conn
	protodata:= new(protos.Command)
	proto.Unmarshal(buf, protodata)
	servlet.Command=protodata
	//fmt.Println("receive request:  "+protodata.String())
	//通过key的第一部分来确定分区
	key:=protodata.GetKey()
	servlet.PartitionKey=key
	if(key!=""){
		index:=strings.Index(key,".")
		if index!=-1 {
			servlet.PartitionKey=key[:index]
		}
		server,_:=c.Get(servlet.PartitionKey)
		//fmt.Println("lookup node for key:"+cmd.PartitionKey+"->"+server)
		if server!=hostname {
			fmt.Println("should never happen")
			//fmt.Println("forward to another node!")
			//Send(server,protodata)
		}else{
			requestChannel <- servlet
		}
	}else{
		requestChannel <- servlet
	}

	return nil
}

func Send(server string, str string){
	//第一次调用时，建立一个连接池
	for destination,_ := range servers{
		if hostname!=destination {
			conn_server,err:=net.Dial("tcp",servers[destination])
			if err!=nil{
				conns[destination]=conn_server
				break
			}
		}
	}
	//发送消息
	if conns[server] !=nil {
		fmt.Fprintf(conns[server],str)
	}
		
}
