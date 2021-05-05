package main

import (
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

type NotFoundError struct {
}

func (e *NotFoundError) Error() string {
	return "Not Found"
}

type AbortError struct {
}

func (e *AbortError) Error() string {
	return "Abort"
}

type TenativeWrite struct {
	Timestamp string
	Value     int
	Committed bool
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
	a.CommitTimestamp = "0:A"
	a.Writes = append(a.Writes, &TenativeWrite{"0:A", 0, false})
	a.Cond = sync.NewCond(&a.Mutex)
}

func (a *Account) Write(value int, timestamp string) error {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	if len(a.Reads) > 0 {
		log.Println(timestamp, a.Reads[len(a.Reads)-1], TimestampGreaterEqual(timestamp, a.Reads[len(a.Reads)-1]))
	}
	if (len(a.Reads) == 0 || TimestampGreaterEqual(timestamp, a.Reads[len(a.Reads)-1])) && TimestampGreater(timestamp, a.CommitTimestamp) {

		for _, write := range a.Writes {
			if write.Timestamp == timestamp {
				write.Value = value
				return nil
			}
		}
		a.Writes = append(a.Writes, &TenativeWrite{timestamp, value, false})
		sort.Slice(a.Writes, func(i, j int) bool {
			return !TimestampGreater(a.Writes[i].Timestamp, a.Writes[j].Timestamp)
		})
		for _, write := range a.Writes {
			log.Println(write.Timestamp)
		}
		return nil
	} else {
		return &AbortError{}
	}
}

func (a *Account) Read(timestamp string) (int, error) {
	a.Mutex.Lock()
	if TimestampGreater(timestamp, a.CommitTimestamp) {
		var tenativeWrite *TenativeWrite
		committed := false
		for i := len(a.Writes) - 1; i >= 0; i-- {
			write := a.Writes[i]
			if TimestampGreaterEqual(timestamp, write.Timestamp) {
				tenativeWrite = write
				committed = write.Committed
				break
			}
		}
		log.Println(tenativeWrite)
		if committed {
			a.Reads = append(a.Reads, timestamp)
			sort.Slice(a.Reads, func(i, j int) bool {
				return !TimestampGreater(a.Reads[i], a.Reads[j])
			})
			log.Println(a.Reads)
			a.Mutex.Unlock()
			return a.Value, nil
		} else {
			if tenativeWrite.Timestamp == timestamp {
				a.Mutex.Unlock()
				return tenativeWrite.Value, nil
			} else if tenativeWrite.Timestamp == "0:A" {
				a.Mutex.Unlock()
				return 0, &NotFoundError{}
			} else {
				a.Cond.Wait()
				a.Mutex.Unlock()
				log.Println("READ UNLOCK")
				return a.Read(timestamp)
			}
		}
	} else {
		a.Mutex.Unlock()
		return 0, &AbortError{}
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
	index := -1
	for i, write := range a.Writes {
		if write.Timestamp == timestamp {
			if i != 1 {
				a.Cond.Wait()
				a.Mutex.Unlock()
				return a.Commit(timestamp)
			} else {
				if write.Value < 0 {
					a.Mutex.Unlock()
					return &AbortError{}
				}
				a.Cond.Broadcast()
				a.Value = write.Value
				write.Committed = true
				index = 1
			}
		}
	}
	log.Println(len(a.Writes))
	if index == 1 {
		a.Writes = a.Writes[1:]
	}
	log.Println(len(a.Writes))
	index = -1
	for i, read := range a.Reads {
		if read == timestamp {
			index = i
			break
		}
	}
	if index >= 0 && index < len(a.Reads)-1 {
		a.Reads = append(a.Reads[:index], a.Reads[index+1:]...)
	} else if index >= 0 {
		a.Reads = a.Reads[:index]
	}
	a.CommitTimestamp = timestamp
	a.Mutex.Unlock()
	return nil
}

func (a *Account) Abort(timestamp string) {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	index := -1
	for i, write := range a.Writes {
		if write.Timestamp == timestamp {
			index = i
			a.Cond.Broadcast()
			break
		}
	}
	if index >= 0 && index < len(a.Writes)-1 {
		a.Writes = append(a.Writes[:index], a.Writes[index+1:]...)
	} else if index >= 0 {
		a.Writes = a.Writes[:index]
	}
	index = -1
	for i, read := range a.Reads {
		if read == timestamp {
			index = i
			break
		}
	}
	if index >= 0 && index < len(a.Reads)-1 {
		a.Reads = append(a.Reads[:index], a.Reads[index+1:]...)
	} else if index >= 0 {
		a.Reads = a.Reads[:index]
	}
}

func TimestampGreaterEqual(timestamp1 string, timestamp2 string) bool {
	return timestamp1 == timestamp2 || TimestampGreater(timestamp1, timestamp2)
}

func TimestampGreater(timestamp1 string, timestamp2 string) bool {
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
