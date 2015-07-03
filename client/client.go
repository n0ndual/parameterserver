package client
import (
	"net"
	"fmt"
	"bufio"
	"time"
	"github.com/tigeress/goredis/command"
	"strconv"
	"strings"
	"os"
	"io"
)
var workerNum=1
var conn net.Conn
func TestClient(){
	time.Sleep(1000 * time.Millisecond)
	conn,err :=net.Dial("tcp","localhost:7777")
	if err!=nil {
		fmt.Println(err)
	}
	conn.Write([]byte(command.EncodeRedisProtocol(command.DecodeText("set clive handsome"))))
	response,_:=bufio.NewReader(conn).ReadString('\n')
	fmt.Println("receive response:  "+response)
	conn.Write([]byte(command.EncodeRedisProtocol(command.DecodeText(("get clive")))))
	response,_=bufio.NewReader(conn).ReadString('\n')
	fmt.Println("receive response:  "+response)
	
}
func PageRank(){
	conn=GetConnection("localhost:7777")
	totalCount:=Get("totalCount")
	keys:=Iterate()
	arr:=strings.Split(keys," ")
	for i:=0;i<2;i++{
		//计算每个节点的rank
		for _,key:= range arr{

			totalCountFloat,_:=strconv.ParseFloat(totalCount,32)
			rank:=0.15/totalCountFloat
			adjs := strings.Split(Get(key+".nodes")," ")
			for _,adj := range adjs{
				crank,_:=strconv.ParseFloat(Get(adj+".crank"),32)
				ncount,_:=strconv.ParseFloat(Get(adj+".count"),32)
				rank+=crank/ncount*0.85
			}
			Set(key+".nrank",strconv.FormatFloat(rank,'f',-1,32))
		}
		fmt.Println("one node compute complete")
		//查看是否所有节点都已经计算完毕。然后每个节点都把crank=nrank。
		//每个节点都同步完成后，再进行下一个超级步
		Incr("computeComplete")
		for{
			time.Sleep(100*time.Millisecond)
			if  Get("computeComplete")==strconv.Itoa(workerNum){
				for _,key:= range arr{
					crank:=Get(key+".nrank")
					Set(key+".crank",crank)
				}
				Incr("updateComplete")
				for{
					if Get("updateComplete")==strconv.Itoa(workerNum){
						break
					}
				}
				break
			}
		}
		fmt.Println("done!")
		Set("computeComplete","0")
		Set("updateComplete","0")
	}
	conn.Close()
}

func LoadWebGraph(){
	time.Sleep(1000*time.Millisecond)
	conn=GetConnection("localhost:7777")
	file,_:=os.Open("/Users/clive/Downloads/web-Stanford.txt")
	reader:=bufio.NewReader(file)
	totalCount:=0
	for {
		line,_,err:=reader.ReadLine()
		if err==io.EOF {
			break
		}
		//fmt.Println("line: "+string(line))
		if line[0]!='#' {
			a:=strings.Split(string(line),"\t")
			prevCountStr:=Get(a[0]+".count")
			var prevCount int=0
//			fmt.Printf("%s\n",prevCountStr)
			if prevCountStr!=""{
				prevCount,_=strconv.Atoi(prevCountStr)
				Set(a[0]+".nodes",Get(a[0]+".nodes")+" "+a[1])
			}else {
				totalCount++
				Set(a[0]+".nodes",a[1])
			}
			Set(a[0]+".count",strconv.Itoa(prevCount+1))

		}
	}
	Set("totalCount", strconv.Itoa(totalCount))
	conn.Close()
}
func SendCommand(conn net.Conn,cmd command.Command) string{
	conn.Write([]byte(command.EncodeRedisProtocol(cmd)))
	response,_,_:=bufio.NewReader(conn).ReadLine()
	return string(response)
	//fmt.Println("receive response:  "+response)
}
func GetConnection(server string) net.Conn{
	conn,err :=net.Dial("tcp",server)
	if err!=nil {
		fmt.Println(err)
	}
	return conn
}

func Get(key string) string{
	cmd:=command.Command{}
	cmd.Argc=2
	cmd.Args=[]string{"get", key}
	value:=SendCommand(conn,cmd)
	return value
}
func Set(key string,value string) string{
	cmd:=command.Command{}
	cmd.Argc=3
	cmd.Args=[]string{"set", key,value}
	response:=SendCommand(conn,cmd)
	return response
}
func Iterate() string{
	cmd:=command.Command{}
	cmd.Argc=1
	cmd.Args=[]string{"iterate"}
	value:=SendCommand(conn,cmd)
	return value
}
func Incr(key string) string{
	cmd:=command.Command{}
	cmd.Argc=2
	cmd.Args=[]string{"incr", key}
	value:=SendCommand(conn,cmd)
	return value
}
