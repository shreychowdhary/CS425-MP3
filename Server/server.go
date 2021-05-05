package main

import (
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var host Node
var nodes Map
var accounts Map
var transactions Map
var numServers int
var serverIds []string

func InitializeServer(hostBranch string, filename string) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	lines := strings.Split(string(content), "\n")
	numServers = len(lines)
	for _, line := range lines {
		serverInfo := strings.Fields(line)
		if len(serverInfo) != 3 {
			log.Fatal("Not enough arguments for line")
		}
		serverIds = append(serverIds, serverInfo[0])
		if serverInfo[0] == hostBranch {
			nodes.Get(hostBranch).(*Node).Port = serverInfo[2]
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
		log.Println("Outgoing: Connected to", branch)
		go Write(&node)
		go Read(&node)
		go HandleServer(&node)
		node.Input <- Packet{false, host.Id, "", CoordinatorResponse, ""}
	}
}

func NewTransaction(clientId string) string {
	transactionId := fmt.Sprintf("%d:%s", time.Now().UnixNano(), host.Id)
	transaction := Transaction{}
	transaction.Init(transactionId, clientId)
	transactions.Set(transactionId, &transaction)
	return transactionId
}

func HandleIncomingConnection(node *Node) {
	packet := <-node.Output
	node.IsClient = packet.IsClient
	node.Id = packet.Id
	if !node.IsClient {
		log.Println("Incoming: Connected to Server", node.Id)
		go HandleServer(node)
		nodes.Set(node.Id, node)
	} else {
		log.Println("Incoming: Connected to Client", node.Id)
		transactionId := NewTransaction(node.Id)
		nodes.Set(node.Id, node)
		go HandleClient(node, transactionId)
		node.Input <- Packet{false, host.Id, transactionId, CoordinatorResponse, "OK"}
	}
}

func HandleCommandFromCoordinator(node *Node, packet Packet) {
	command := ParseCommand(packet.Command)
	if !transactions.Contains(packet.TransactionId) {
		transaction := Transaction{}
		transaction.Init(packet.TransactionId, "")
		transactions.Set(packet.TransactionId, &transaction)
	}
	transaction := transactions.Get(packet.TransactionId).(*Transaction)
	switch command.Action {
	case "DEPOSIT":
		if !accounts.Contains(command.Account) {
			transaction.AddCreatedAccount(command.Account)
			account := Account{}
			account.Init(command.Account)
			account.Write(0, packet.TransactionId)
			accounts.Set(command.Account, &account)
		}
		transaction.AddAccount(command.Account)
		account := accounts.Get(command.Account).(*Account)
		value, err := account.Read(packet.TransactionId)
		if _, ok := err.(*NotFoundError); ok {
			account.Write(0, packet.TransactionId)
		} else if err != nil {
			node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantAbort, "ABORTED"}
			return
		}
		err = account.Write(value+command.Value, packet.TransactionId)
		if err != nil {
			node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantAbort, "ABORTED"}
			return
		}
		log.Println("Final Value:", value)
		accounts.Set(command.Account, account)
		node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantResponse, "OK"}
	case "BALANCE":
		if !accounts.Contains(command.Account) {
			node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantAbort, "NOT FOUND, ABORTED"}
			return
		}
		transaction.AddAccount(command.Account)
		account := accounts.Get(command.Account).(*Account)
		value, err := account.Read(packet.TransactionId)
		if _, ok := err.(*NotFoundError); ok {
			node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantAbort, "NOT FOUND, ABORTED"}
			return
		} else if err != nil {
			node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantAbort, "ABORTED"}
			return
		}
		node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantResponse, fmt.Sprintf("%s.%s = %d", command.Branch, command.Account, value)}
	case "WITHDRAW":
		if !accounts.Contains(command.Account) {
			node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantAbort, "NOT FOUND, ABORTED"}
			return
		}
		transaction.AddAccount(command.Account)
		account := accounts.Get(command.Account).(*Account)
		value, err := account.Read(packet.TransactionId)
		if _, ok := err.(*NotFoundError); ok {
			node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantAbort, "NOT FOUND, ABORTED"}
			return
		} else if err != nil {
			node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantAbort, "ABORTED"}
			return
		}
		err = account.Write(value-command.Value, packet.TransactionId)
		if err != nil {
			node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantAbort, "ABORTED"}
			return
		}
		node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantResponse, "OK"}
	}
}

func HandleResponseFromParticipant(node *Node, packet Packet) {
	clientId := transactions.Get(packet.TransactionId).(*Transaction).ClientId
	clientNode := nodes.Get(clientId).(*Node)
	log.Println(clientNode.Id, packet.Command, host.Id)
	clientNode.Input <- Packet{false, host.Id, packet.TransactionId, CoordinatorResponse, packet.Command}
}

func HandlePrepareFromCoordinator(node *Node, packet Packet) {
	if !transactions.Contains(packet.TransactionId) {
		node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantYes, "YES"}
		return
	}
	transaction := transactions.Get(packet.TransactionId).(*Transaction)
	if transaction.NumAccounts() == 0 {
		node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantYes, "YES"}
	}
	for _, accountId := range transaction.GetAccounts() {
		account := accounts.Get(accountId).(*Account)
		if account.CanCommit(packet.TransactionId) {
			node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantYes, "YES"}
		} else {
			account.Abort(packet.TransactionId)
			node.Input <- Packet{false, host.Id, packet.TransactionId, ParticipantAbort, "ABORTED"}
		}
	}
}

func HandleCommitFromCoordinator(node *Node, packet Packet) {
	if !transactions.Contains(packet.TransactionId) {
		return
	}
	transaction := transactions.Get(packet.TransactionId).(*Transaction)
	transaction.SetState(Committed)
	for _, accountId := range transaction.GetAccounts() {
		account := accounts.Get(accountId).(*Account)
		account.Commit(packet.TransactionId)
	}
	PrintBalances()
}

func HandleYesFromParticipant(node *Node, packet Packet) {
	transaction := transactions.Get(packet.TransactionId).(*Transaction)
	if transaction.GetState() != Prepare {
		return
	}
	transaction.AddResponse(node.Id)
	if transaction.NumResponses() == numServers {
		for _, id := range serverIds {
			node := nodes.Get(id).(*Node)
			node.Input <- Packet{false, host.Id, packet.TransactionId, CoordinatorCommit, "Commit"}
		}
		clientNode := nodes.Get(transaction.GetClientId()).(*Node)
		clientNode.Input <- Packet{false, host.Id, packet.TransactionId, CoordinatorResponse, "COMMIT OK"}
	}
}

func HandleAbortFromParticipant(node *Node, packet Packet) {
	transaction := transactions.Get(packet.TransactionId).(*Transaction)
	clientNode := nodes.Get(transaction.GetClientId()).(*Node)
	clientNode.Input <- Packet{false, host.Id, packet.TransactionId, CoordinatorResponse, packet.Command}
	SendAbortToParticipants(packet.TransactionId)
}

func HandleAbortFromCoordinator(node *Node, packet Packet) {
	log.Println("ABORTED:", packet.TransactionId, transactions.Contains(packet.TransactionId))
	if !transactions.Contains(packet.TransactionId) {
		return
	}
	transaction := transactions.Get(packet.TransactionId).(*Transaction)
	if transaction.GetState() == Aborted {
		return
	}
	for _, accountId := range transaction.GetAccounts() {
		log.Println("Aborting:", accountId)
		account := accounts.Get(accountId).(*Account)
		account.Abort(packet.TransactionId)
	}
	transaction.State = Aborted
}

func SendPrepareToParticipants(transactionId string) {
	transaction := transactions.Get(transactionId).(*Transaction)
	transaction.SetState(Prepare)
	for _, id := range serverIds {
		node := nodes.Get(id).(*Node)
		node.Input <- Packet{false, host.Id, transactionId, CoordinatorPrepare, "Prepare"}
	}
}

func SendAbortToParticipants(transactionId string) {
	for _, id := range serverIds {
		node := nodes.Get(id).(*Node)
		node.Input <- Packet{false, host.Id, transactionId, CoordinatorAbort, "ABORTED"}
	}
}

func PrintBalances() {
	accounts.RWMutex.RLock()
	keys := make([]string, len(accounts.Data))
	i := 0
	for k := range accounts.Data {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	balanceString := "BALANCES "
	for _, id := range keys {
		account := accounts.Data[id].(*Account)
		account.Mutex.Lock()
		if account.Value > 0 {
			balanceString += fmt.Sprintf("%s:%d ", id, account.Value)
		}
		account.Mutex.Unlock()
	}
	fmt.Println(balanceString)
	accounts.RWMutex.RUnlock()

}

func HandleServer(node *Node) {
	for {
		packet := <-node.Output
		log.Println(packet.Command)
		switch packet.CommandType {
		case CoordinatorRequest:
			go HandleCommandFromCoordinator(node, packet)
		case CoordinatorPrepare:
			go HandlePrepareFromCoordinator(node, packet)
		case CoordinatorCommit:
			go HandleCommitFromCoordinator(node, packet)
		case CoordinatorAbort:
			go HandleAbortFromCoordinator(node, packet)
		case ParticipantResponse:
			go HandleResponseFromParticipant(node, packet)
		case ParticipantYes:
			go HandleYesFromParticipant(node, packet)
		case ParticipantAbort:
			go HandleAbortFromParticipant(node, packet)
		}
	}
}

func HandleClient(node *Node, transactionId string) {
	for {
		packet := <-node.Output
		if packet.TransactionId != transactionId {
			log.Println("Transaction Id doesn't match")
			return
		}
		command := ParseCommand(packet.Command)
		log.Println(command.Action)
		switch command.Action {
		case "BEGIN":
			NewTransaction(node.Id)
			node.Input <- Packet{false, host.Id, transactionId, CoordinatorResponse, "OK"}
		case "DEPOSIT":
			SendPacketToParticipant(command.Branch, Packet{false, host.Id, transactionId, CoordinatorRequest, packet.Command})
		case "BALANCE":
			SendPacketToParticipant(command.Branch, Packet{false, host.Id, transactionId, CoordinatorRequest, packet.Command})
		case "WITHDRAW":
			SendPacketToParticipant(command.Branch, Packet{false, host.Id, transactionId, CoordinatorRequest, packet.Command})
		case "COMMIT":
			SendPrepareToParticipants(transactionId)
		case "ABORT":
			SendAbortToParticipants(transactionId)
			node.Input <- Packet{false, host.Id, transactionId, CoordinatorResponse, "ABORTED"}
		default:
			node.Input <- Packet{false, host.Id, transactionId, CoordinatorResponse, "INVALID COMMAND"}
		}
	}
}

func SendPacketToParticipant(server string, packet Packet) {
	node := nodes.Get(server).(*Node)
	node.Input <- packet
}

func ParseCommand(command string) Command {
	commandInfo := strings.Fields(command)
	if len(commandInfo) == 1 {
		return Command{commandInfo[0], "", "", 0}
	} else if len(commandInfo) == 2 {
		accountInfo := strings.Split(commandInfo[1], ".")
		return Command{commandInfo[0], accountInfo[0], accountInfo[1], 0}
	} else if len(commandInfo) == 3 {
		accountInfo := strings.Split(commandInfo[1], ".")
		value, err := strconv.Atoi(commandInfo[2])
		if err != nil {
			log.Println(err)
		}
		return Command{commandInfo[0], accountInfo[0], accountInfo[1], value}
	} else {
		log.Panic(command)
		return Command{}
	}
}

func Write(node *Node) {
	encoder := gob.NewEncoder(node.Connection)
	for {
		packet := <-node.Input
		log.Printf("Send:%s %s->%s\n", packet.Command, host.Id, node.Id)
		if node.IsHost {
			node.Output <- packet
		} else {
			err := encoder.Encode(packet)
			if err != nil {
				log.Println(err)
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
			return
		}
		node.Output <- packet
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if len(os.Args) != 3 {
		log.Fatal("Format should be ./server branch configuration")
	}
	nodes.Init()
	accounts.Init()
	transactions.Init()

	host = Node{
		Id:       os.Args[1],
		IsHost:   true,
		IsClient: false,
		Input:    make(chan Packet, 100),
		Output:   make(chan Packet, 100),
	}
	go Write(&host)
	go HandleServer(&host)
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
		go Read(&node)
		go Write(&node)
		go HandleIncomingConnection(&node)
	}
}
