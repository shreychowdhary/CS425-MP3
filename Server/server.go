package main

import (
	"encoding/gob"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

var host Node
var nodes NodeMap
var accounts AccountMap

func InitializeServer(hostBranch string, filename string) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		serverInfo := strings.Fields(line)
		if len(serverInfo) != 3 {
			log.Fatal("Not enough arguments for line")
		}
		if serverInfo[0] == hostBranch {
			nodes.Get(hostBranch).Port = serverInfo[2]
		} else {
			go ConnectToServer(serverInfo[0], serverInfo[1], serverInfo[2])
		}
	}
}

func ConnectToServer(branch string, ip string, port string) {
	for !nodes.Contains(branch) {
		connection, err := net.Dial("tcp", ip+":"+port)
		if err != nil {
			log.Println("Unable to connect to", branch, ip, port)
			time.Sleep(5 * time.Second)
			continue
		}
		node := Node{
			Id:         branch,
			Address:    ip,
			Port:       port,
			Connection: connection,
			IsHost:     false,
			IsClient:   false,
			Input:      make(chan Packet, 100),
			Output:     make(chan Packet, 100),
		}
		nodes.Set(branch, &node)
		log.Println("Connected to", branch)
		go Write(&node)
		go Read(&node)
		node.Input <- Packet{false, host.Id, ""}
	}
}

func HandleIncomingConnection(node *Node) {
	packet := <-node.Output
	node.IsClient = packet.IsClient
	node.Id = packet.Id
	if !node.IsClient {
		log.Println("Connected to Server", node.Id)
		go HandleServer(node)
	} else {
		log.Println("Connected to Client", node.Id)
		go HandleClient(node)
		node.Input <- Packet{false, host.Id, "OK"}
	}
	nodes.Set(node.Id, node)
}

func HandleServer(node *Node) {

}

func HandleClient(node *Node) {
	packet := <-node.Output
	ParseCommand(packet.Command)
}

func ParseCommand(command string) {

}

func Write(node *Node) {
	encoder := gob.NewEncoder(node.Connection)
	for {
		packet := <-node.Input
		if node.IsHost {
			node.Output <- packet
		} else {
			err := encoder.Encode(packet)
			if err != nil {
				nodes.Delete(node.Id)
				return
			}
		}
	}
}

func Read(node *Node) {
	decoder := gob.NewDecoder(node.Connection)
	for {
		var packet Packet
		err := decoder.Decode(&packet)
		if err != nil {
			log.Println(err)
			nodes.Delete((node.Id))
			return
		}
		node.Output <- packet
	}
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Format should be ./server branch configuration")
	}
	nodes.Init()
	accounts.Init()

	host = Node{
		Id:       os.Args[1],
		IsHost:   true,
		IsClient: false,
		Input:    make(chan Packet, 100),
		Output:   make(chan Packet, 100),
	}
	nodes.Set(host.Id, &host)

	InitializeServer(os.Args[1], os.Args[2])

	listen, err := net.Listen("tcp", ":"+host.Port)
	if err != nil {
		log.Fatal(err)
	}
	for {
		connection, err := listen.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		node := Node{
			Connection: connection,
			IsHost:     false,
			Input:      make(chan Packet, 100),
			Output:     make(chan Packet, 100),
		}
		go HandleIncomingConnection(&node)
		go Read(&node)
		go Write(&node)
	}
}
