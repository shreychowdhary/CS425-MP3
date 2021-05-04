package main

import (
	"errors"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type CommandType int

const (
	ClientRequest CommandType = iota
	CoordinatorResponse
	CoordinatorRequest
	CoordinatorPrepare
	CoordinatorCommit
	CoordinatorAbort
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

type Map struct {
	RWMutex sync.RWMutex
	Data    map[string]interface{}
}

func (m *Map) Init() {
	m.Data = make(map[string]interface{})
}

func (m *Map) Delete(id string) {
	m.RWMutex.Lock()
	delete(m.Data, id)
	m.RWMutex.Unlock()
}

func (m *Map) Set(id string, value interface{}) {
	m.RWMutex.Lock()
	m.Data[id] = value
	m.RWMutex.Unlock()
}

func (m *Map) Get(id string) interface{} {
	m.RWMutex.RLock()
	defer m.RWMutex.RUnlock()
	value := m.Data[id]
	return value
}

func (m *Map) Length() int {
	m.RWMutex.RLock()
	length := len(m.Data)
	m.RWMutex.RUnlock()
	return length
}

func (m *Map) Contains(id string) bool {
	m.RWMutex.RLock()
	_, ok := m.Data[id]
	m.RWMutex.RUnlock()
	return ok
}

type TenativeWrite struct {
	Timestamp string
	Value     int
}

type Account struct {
	Id              string
	Value           int
	CommitTimestamp string
	Reads           []string
	Writes          []*TenativeWrite
	Mutex           sync.Mutex
	Cond            *sync.Cond
}

func (a *Account) Init(id string) {
	a.Id = id
	a.Writes = append(a.Writes, &TenativeWrite{"", 0})
	a.Cond = sync.NewCond(&a.Mutex)
}

func (a *Account) Write(value int, timestamp string) error {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	log.Println(timestamp, a.Reads[len(a.Reads)-1], a.CommitTimestamp)
	if (len(a.Reads) == 0 || TimestampGreaterEqual(timestamp, a.Reads[len(a.Reads)-1])) && TimestampGreater(timestamp, a.CommitTimestamp) {
		for _, write := range a.Writes {
			log.Println(write.Timestamp, timestamp)
			if write.Timestamp == timestamp {
				write.Value = value
				return nil
			}
		}
		a.Writes = append(a.Writes, &TenativeWrite{timestamp, value})
		sort.Slice(a.Writes, func(i, j int) bool {
			return !TimestampGreater(a.Writes[i].Timestamp, a.Writes[j].Timestamp)
		})
		return nil
	} else {
		return errors.New("Abort")
	}
}

func (a *Account) Read(timestamp string) (int, error) {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	if TimestampGreater(timestamp, a.CommitTimestamp) {
		var tenativeWrite *TenativeWrite
		committed := false
		for i := len(a.Writes) - 1; i >= 0; i-- {
			write := a.Writes[i]
			if TimestampGreaterEqual(timestamp, write.Timestamp) {
				tenativeWrite = write
				committed = TimestampGreaterEqual(a.CommitTimestamp, write.Timestamp)
				break
			}
		}
		if committed {
			a.Reads = append(a.Reads, timestamp)
			sort.Slice(a.Writes, func(i, j int) bool {
				return !TimestampGreater(a.Reads[i], a.Reads[j])
			})
			return a.Value, nil
		} else {
			if tenativeWrite.Timestamp == timestamp {
				return tenativeWrite.Value, nil
			} else {
				a.Cond.Wait()
				a.Mutex.Unlock()
				return a.Read(timestamp)
			}
		}
	} else {
		return 0, errors.New("Abort")
	}
}

func (a *Account) CanCommit(timestamp string) bool {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	for _, write := range a.Writes {
		if write.Timestamp == timestamp {
			return write.Value >= 0
		}
	}
	return true
}

func (a *Account) Commit(timestamp string) error {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	for i, write := range a.Writes {
		if write.Timestamp == timestamp {
			if i != 1 {
				a.Cond.Wait()
				a.Mutex.Unlock()
				return a.Commit(timestamp)
			} else {
				if write.Value < 0 {
					return errors.New("Abort")
				}
				a.Value = write.Value
			}
		}
	}
	a.Writes = a.Writes[1:]
	var index int
	for i, read := range a.Reads {
		if read == timestamp {
			index = i
			break
		}
	}
	if index < len(a.Reads)-1 {
		a.Reads = append(a.Reads[:index], a.Reads[index+1:]...)
	} else {
		a.Reads = a.Reads[:index]
	}
	a.CommitTimestamp = timestamp
	return nil
}

func (a *Account) Abort(timestamp string) {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	var index int
	for i, write := range a.Writes {
		if write.Timestamp == timestamp {
			index = i
			break
		}
	}
	if index < len(a.Writes)-1 {
		a.Writes = append(a.Writes[:index], a.Writes[index+1:]...)
	} else {
		a.Writes = a.Writes[:index]
	}
	for i, read := range a.Reads {
		if read == timestamp {
			index = i
			break
		}
	}
	if index < len(a.Reads)-1 {
		a.Reads = append(a.Reads[:index], a.Reads[index+1:]...)
	} else {
		a.Reads = a.Reads[:index]
	}
}

func TimestampGreaterEqual(timestamp1 string, timestamp2 string) bool {
	return timestamp1 == timestamp2 || TimestampGreater(timestamp1, timestamp2)
}

func TimestampGreater(timestamp1 string, timestamp2 string) bool {
	if timestamp2 == "" {
		return true
	} else if timestamp1 == "" {
		return false
	}
	timestampInfo1 := strings.Split(timestamp1, ":")
	timestampInfo2 := strings.Split(timestamp2, ":")
	physicalTimestamp1, _ := strconv.Atoi(timestampInfo1[0])
	physicalTimestamp2, _ := strconv.Atoi(timestampInfo2[0])
	if physicalTimestamp1 > physicalTimestamp2 {
		return true
	} else if physicalTimestamp1 < physicalTimestamp2 {
		return false
	} else {
		return timestampInfo1[1] > timestampInfo2[1]
	}
}

type Command struct {
	Action  string
	Branch  string
	Account string
	Value   int
}

type Transaction struct {
	Id              string
	ClientId        string
	Accounts        map[string]bool
	CreatedAccounts []string
	State           TransactionState
	Responses       map[string]bool
}

type TransactionState int

const (
	Open TransactionState = iota
	Prepare
	Committed
	Aborted
)
