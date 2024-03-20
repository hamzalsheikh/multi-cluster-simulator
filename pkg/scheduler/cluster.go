package scheduler

import (
	"fmt"
	"sync"
	"time"
)

type Cluster struct {
	Id                uint
	Nodes             []Node
	URL               string
	TotalMemory       uint
	TotalCore         uint
	MemoryUtilization uint
	CoreUtilization   uint
	resourceMutex     *sync.Mutex
}

func (c *Cluster) SetTotalResources() error {
	// function part of cluster initialization

	c.resourceMutex.Lock()
	defer c.resourceMutex.Unlock()

	c.TotalCore = 0
	c.TotalMemory = 0
	for _, node := range c.Nodes {
		c.TotalCore += uint(len(node.Cores))
		c.TotalMemory += node.Memory
	}

	return nil
}

func (c *Cluster) GetResourceUtilization() (core uint, mem uint) {
	c.resourceMutex.Lock()
	defer c.resourceMutex.Unlock()
	c.CoreUtilization = 0
	c.MemoryUtilization = 0
	for _, node := range c.Nodes {
		node.mutex.Lock()
		c.CoreUtilization += node.CoresAvailable
		c.MemoryUtilization += node.MemoryAvailable
		node.mutex.Unlock()
	}
	return c.CoreUtilization, c.MemoryUtilization
}

type Node struct {
	Id              uint
	Type            string
	URL             string
	Memory          uint
	Cores           []uint
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
	fmt.Printf("node %v running job %v\n", n.Id, j.Id)
	time.Sleep(j.Duration)

	n.mutex.Lock()
	delete(n.RunningJobs, j.Id)
	n.CoresAvailable += j.CoresNeeded
	n.MemoryAvailable += j.MemoryNeeded
	n.mutex.Unlock()
	fmt.Printf("node %v finished job %v\n", n.Id, j.Id)
	// inform scheduler that you're done
	// should be a http request, but call the function directly for now
	sched.JobFinished(j)
}
