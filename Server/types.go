package main

import (
	"net"
	"sync"
	"time"
)

type Packet struct {
	IsClient bool
	Id       string
	Command  string
}

type Node struct {
	Id         string
	Address    string
	Port       string
	Connection net.Conn
	Input      chan Packet
	Output     chan Packet
	IsHost     bool
	IsClient   bool
}

type NodeMap struct {
	RWMutex sync.RWMutex
	Nodes   map[string]*Node
}

func (m *NodeMap) Init() {
	m.Nodes = make(map[string]*Node)
}

func (m *NodeMap) Delete(id string) {
	m.RWMutex.Lock()
	delete(m.Nodes, id)
	m.RWMutex.Unlock()
}

func (m *NodeMap) Set(id string, node *Node) {
	m.RWMutex.Lock()
	m.Nodes[id] = node
	m.RWMutex.Unlock()
}

func (m *NodeMap) Get(id string) *Node {
	m.RWMutex.RLock()
	node := m.Nodes[id]
	m.RWMutex.RUnlock()
	return node
}

func (m *NodeMap) Length() int {
	m.RWMutex.RLock()
	length := len(m.Nodes)
	m.RWMutex.RUnlock()
	return length
}

func (m *NodeMap) Contains(id string) bool {
	m.RWMutex.RLock()
	_, ok := m.Nodes[id]
	m.RWMutex.RUnlock()
	return ok
}

type Account struct {
	Id              string
	Value           int
	CommitTimestamp time.Time
	ReadTimestamps  []time.Time
	TenativeWrites  []time.Time
}

type AccountMap struct {
	Accounts map[string]*Account
}

func (m *AccountMap) Init() {
	m.Accounts = make(map[string]*Account)
}
