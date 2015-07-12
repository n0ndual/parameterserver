package main

import (
	"strconv"
	"net"
	"fmt"
	"time"
	"stathat.com/c/consistent"
	"github.com/scorpionis/parameterserver/client"
	"flag"
	"bufio"
	"strings"
)

//把自己注册到dht中
var c  =consistent.New()
var server string
var ip string
var port string
var masterip string
var masterport string

func Init(){
	//flags
	flag.StringVar(&masterip,"masterip","7.7.7.1","server ip address")
	flag.StringVar(&masterport,"masterport",":7778","server tcp listen port")
	flag.StringVar(&ip,"ip","7.7.7.1","server ip address")
	flag.StringVar(&port,"port",":7778","server tcp listen port")
	flag.Parse()
	server = ip+port
	fmt.Println(server)
	//fmt.Println(os.Args)
}
func main(){
	Init()
	client.ConsistentHash=consistent.New()
	client.ConsistentHash.Add("7.7.7.1")
	client.ConsistentHash.Add("7.7.7.7")
	conn1,_:=net.Dial("tcp","7.7.7.1:7777")
	conn2,_:=net.Dial("tcp","7.7.7.7:7777")
	client.Conns["7.7.7.1"]=conn1
	client.Conns["7.7.7.7"]=conn2
	client.Conns["localhost"]=client.Conns[ip]

	fmt.Println(flag.Args())
	if flag.Args()[1]=="master" {
		StartServer()
	}
	if flag.Args()[1]=="worker" {
		StartWorker()
	}

}
var workers []net.Conn
var chan1 chan string = make(chan string)

func HandleConn(conn net.Conn){
	message,_:=bufio.NewReader(conn).ReadString('\n')
	fmt.Println(message)
	if strings.Contains(string(message),"connect"){
		workers=append(workers,conn)
		if len(workers)==2{
//			fmt.Println("go")
			chan1 <- "go"
		}
	}
}

func StartServer(){
	//第一步：连接两个worker
	//第二步，开始计算pagerank，map操作
	//第三步，同步点：开始聚合操作
	//第四步，等待同时完成
	go Master()
	ln,err:=net.Listen("tcp",server)
	if err!=nil {
		fmt.Println(err)
	}
	fmt.Println("listening on tcp "+server)
	for {
		conn,err:=ln.Accept()
		if err!=nil {
			fmt.Println(err)
		}

		go HandleConn(conn)
	}
	

}
func Master(){
	//开始分配任务
	mes:= <- chan1
	if mes=="go"{
		fmt.Println("job start at: "+time.Now().Format("2006-01-02 15:04:05"))
		for step:=0;step<50;step++ {

			for _,c:=range workers{
				//bufio.NewWriter(c).WriteString("map")
				fmt.Fprintf(c,"compute"+"\n")
			}
			//fmt.Println("wait here")
			if strings.Contains(ReadMessage(workers[0]),"computeDone") &&
				strings.Contains(ReadMessage(workers[1]),"computeDone"){
				for _,c:=range workers{
					fmt.Fprintf(c,"update"+"\n")
				}	
			}
			if strings.Contains(ReadMessage(workers[0]),"updateDone") &&
				strings.Contains(ReadMessage(workers[1]),"updateDone"){
				//do nothing
				fmt.Println("iterate "+strconv.Itoa(step)+" complete")
			}
		}
		fmt.Println("job done at: "+time.Now().Format("2006-01-02 15:04:05"))
		fmt.Println("job  finished!")
	}
}
func ReadMessage(conn net.Conn) string{
	message,_:=bufio.NewReader(conn).ReadString('\n')
	return message
}
func StartWorker(){
	conn1,_:=net.Dial("tcp",masterip+masterport)
	fmt.Fprintf(conn1,"connect"+"\n")
	for{
		message,_:=bufio.NewReader(conn1).ReadString('\n')
		//fmt.Println(message)
		if strings.Contains(string(message),"compute") {
		//	fmt.Println("do compute")
			client.PageRankMap()
			fmt.Fprintf(conn1,"computeDone"+"\n")
		}
		if strings.Contains(string(message),"update"){
			client.PageRankReduce()
			fmt.Fprintf(conn1,"updateDone"+"\n")
		}
	}
}
