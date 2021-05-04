package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"time"

	"log"
	"math/rand"
	"net"
	"os"
	"strings"
)

type CommandType int

const (
	ClientRequest CommandType = iota
	CoordinatorResponse
	CoordinatorRequest
	CoordinatorPrepare
	CoordinatorCommit
	ParticipantResponse
	ParticipantYes
	ParticipantAbort
)

type Packet struct {
	IsClient      bool
	Id            string
	TransactionId string
	CommandType   CommandType
	Command       string
}

func ChooseServer(filename string) (net.Conn, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal("Unable to read configuration file")
	}
	lines := strings.Split(string(content), "\n")
	index := rand.Intn(len(lines))
	serverInfo := strings.Fields(lines[index])
	log.Printf("Connecting to Branch %s", serverInfo[0])
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
			return
		}
		output <- packet
	}
}

func HandlePacket(packet Packet) (bool, string) {
	if strings.Contains(packet.Command, "ABORTED") {
		return false, packet.Command
	} else if packet.Command == "COMMIT OK" {
		return false, packet.Command
	}
	return true, packet.Command
}

func main() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if len(os.Args) != 3 {
		log.Fatal("Format should be ./client id configuration")
	}
	var connection net.Conn
	var err error
	var response string
	var output chan Packet
	var input chan Packet
	id := os.Args[1]
	scanner := bufio.NewScanner(os.Stdin)
	inTransaction := false
	transactionId := ""
	for scanner.Scan() {
		command := scanner.Text()
		command = strings.TrimSpace(command)
		log.Println(command)
		if command == "BEGIN" {
			if connection != nil {
				connection.Close()
			}
			connection, err = ChooseServer(os.Args[2])
			if err != nil {
				log.Println("Unable to connect")
				continue
			}
			input = make(chan Packet, 100)
			output = make(chan Packet, 100)
			go WriteServer(connection, input)
			go ReadServer(connection, output)
			inTransaction = true
			transactionId = ""
		} else if !inTransaction {
			continue
		}
		input <- Packet{true, id, transactionId, ClientRequest, command}
		packet := <-output
		transactionId = packet.TransactionId
		inTransaction, response = HandlePacket(packet)
		fmt.Println(response)
	}
}
