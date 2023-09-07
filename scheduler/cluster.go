package scheduler

import (
	"fmt"
	"sync"
	"time"
)

type Cluster struct {
	Id    uint
	Nodes []Node
	URL   string
}

type Node struct {
	Id     uint
	Type   string
	URL    string
	Memory uint
	Cores  []uint

	MemoryAvailable uint
	CoresAvailable  uint
	RunningJobs     map[uint]Job
	mutex           *sync.Mutex
}

// simulate running a job
func (n *Node) RunJob(j Job) {

	// update node state
	n.mutex.Lock()
	n.RunningJobs[j.Id] = j
	n.CoresAvailable -= j.CoresNeeded
	n.MemoryAvailable -= j.MemoryNeeded
	n.mutex.Unlock()
	fmt.Printf("node %v running job %v", n.Id, j.Id)
	time.Sleep(j.Duration)

	n.mutex.Lock()
	delete(n.RunningJobs, j.Id)
	n.CoresAvailable += j.CoresNeeded
	n.MemoryAvailable += j.MemoryNeeded
	n.mutex.Unlock()
	fmt.Printf("node %v finished job %v", n.Id, j.Id)
}
