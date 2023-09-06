package scheduler

import (
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

	time.Sleep(j.duration)

	n.mutex.Lock()
	delete(n.RunningJobs, j.Id)
	n.CoresAvailable += j.CoresNeeded
	n.MemoryAvailable += j.MemoryNeeded
	n.mutex.Unlock()
}
