package main

import (
	"net"
	"fmt"
	"time"
	"stathat.com/c/consistent"
	"github.com/tigeress/goredis/command"
	"os"
	"github.com/tigeress/goredis/client"
	"flag"
	"bufio"
	"strings"
	"strconv"
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
	hostname,_ =os.Hostname()
	servers=make(map[string]string)
	servers["clive"]=ip
//	servers["clive0"]="7.7.7.7"
//	servers["clive1"]="7.7.7.8"
//	servers["clive2"]="7.7.7.9"
	for k,_ :=range servers{
		c.Add(k)
	}
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
		time.Sleep(1000*time.Second)
	}
}

func StartServer(){
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
		go HandleConnection(conn)
	}
}

func HandleConnection(conn net.Conn){
	for {
		var cmd *command.Command = command.DecodeRedisProtocol(conn)
		if cmd == nil {
			break
		}
		//fmt.Println("recv: "+ command.EncodeText(*cmd))
		//通过key的第一部分来确定分区
		if(cmd.Argc>1){
			index:=strings.Index(cmd.Args[1],".")
			if index==-1 {
				cmd.PartitionKey=cmd.Args[1]
			}else{
				cmd.PartitionKey=cmd.Args[1][:index]
			}
			server,_:=c.Get(cmd.PartitionKey)
			//fmt.Println("lookup node for key:"+cmd.PartitionKey+"->"+server)
			if server!=hostname {
				//fmt.Println("forward to another node!")
				Send(server,command.EncodeRedisProtocol(*cmd))
			}else{
				DoCommand(conn,*cmd)
			}
		}else{
			DoCommand(conn,*cmd)
		}
	}

}
func DoCommand(conn net.Conn,cmd command.Command){
	if cmd.Args[0]=="set" {
		kv[cmd.Args[1]]=cmd.Args[2]
		keys[cmd.PartitionKey]=1;
		conn.Write([]byte("succ\n"))
	}
	if cmd.Args[0]=="get" {
		value:=kv[cmd.Args[1]]
		conn.Write([]byte(value+"\n"))
	}
	if cmd.Args[0]=="remove" {
		delete(kv,cmd.Args[0])
		delete(keys,cmd.PartitionKey)
		conn.Write([]byte("succ\n"))
	}
	if(cmd.Args[0]=="update"){
		kv[cmd.Args[1]]=cmd.Args[2]
		keys[cmd.PartitionKey]=1;
		conn.Write([]byte("succ\n"))
	}
	if(cmd.Args[0]=="iterate"){
		//一次取出所有的key吗？只取partitionKey
		var result string=""
		for k,_:=range keys{
//			fmt.Println("key: "+k)
			result=result+k+" "
		}
		//fmt.Println("result:"+result)
		result=result[0:len(result)-1]
		conn.Write([]byte(result+"\n"))
	}
	if(cmd.Args[0]=="incr"){
		if kv[cmd.Args[1]]=="" {
			kv[cmd.Args[1]]="1"
		}else{
			prev,_:=strconv.Atoi(kv[cmd.Args[1]])
			kv[cmd.Args[1]]=strconv.Itoa(prev+1)
		}
		conn.Write([]byte("succ\n"))
	}
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

