package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io/ioutil"

	"log"
	"math/rand"
	"net"
	"os"
	"strings"
)

type Packet struct {
	IsClient bool
	Id       string
	Command  string
}

func ChooseServer(filename string) (net.Conn, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal("Unable to read configuration file")
	}
	lines := strings.Split(string(content), "\n")
	index := rand.Intn(len(lines))
	serverInfo := strings.Fields(lines[index])
	log.Printf("Connected to Branch %s", serverInfo[0])
	if len(serverInfo) != 3 {
		log.Fatal("Invalid config file")
	}
	return net.Dial("tcp", serverInfo[1]+":"+serverInfo[2])
}

func WriteServer(connection net.Conn, input chan Packet) {
	encoder := gob.NewEncoder(connection)
	for {
		packet := <-input
		err := encoder.Encode(packet)
		if err != nil {
			return
		}
	}
}

func ReadServer(connection net.Conn, output chan Packet) {
	decoder := gob.NewDecoder(connection)
	for {
		var packet Packet
		err := decoder.Decode(&packet)
		if err != nil {
			log.Println(err)
			return
		}
		output <- packet
	}
}

func HandlePacket(packet Packet) (bool, string) {
	if strings.Contains(packet.Command, "ABORTED") {
		return false, packet.Command
	}
	return true, packet.Command
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Format should be ./client id configuration")
	}
	var connection net.Conn
	var response string
	id := os.Args[1]
	output := make(chan Packet, 100)
	input := make(chan Packet, 100)
	reader := bufio.NewReader(os.Stdin)
	inTransaction := false
	for {
		command, err := reader.ReadString('\n')
		command = strings.TrimSuffix(command, "\n")
		log.Println(command)
		if err != nil {
			log.Fatal(err)
		}
		if command == "BEGIN" {
			connection, err = ChooseServer(os.Args[2])
			if err != nil {
				log.Println("Unable to connect")
				continue
			}
			go WriteServer(connection, input)
			go ReadServer(connection, output)
			inTransaction = true
		} else if !inTransaction {
			continue
		}
		input <- Packet{true, id, command}
		packet := <-output
		inTransaction, response = HandlePacket(packet)
		fmt.Println(response)
	}
}
