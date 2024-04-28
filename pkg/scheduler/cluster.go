package scheduler

import (
	"fmt"
	"math"
	"sync"
	"time"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
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

func (c *Cluster) GetTotalResources() (uint, uint) {
	c.resourceMutex.Lock()
	defer c.resourceMutex.Unlock()
	return c.TotalCore, c.TotalMemory
}

func (c *Cluster) GetResourceUtilization() (uint, uint) {
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

func (c *Cluster) AddVirtualNode(node *pb.NodeObject) {
	var n Node
	n.Type = "Virtual"
	n.CoresAvailable = uint(node.Cores)
	n.MemoryAvailable = uint(node.Memory)
	n.Time = uint(node.Time)
	n.URL = node.Url
	//
	//c.Nodes = append(c.Nodes, n)
	sched.ScheduleJobsOnVirtual(&n)
}

func (c *Cluster) AllocateVirtualNodeResources(req *pb.VirtualNodeRequest) *pb.NodeObject {
	for _, node := range c.Nodes {
		if req.Memory <= 0 && req.Cores <= 0 {
			break
		}
		node.mutex.Lock()
		var mem_diff float64
		var core_diff float64
		if req.Memory > 0 {
			mem_diff = math.Abs(float64(req.Memory) - float64(node.MemoryAvailable))
		}

		if req.Cores > 0 {
			core_diff = math.Abs(float64(req.Cores) - float64(node.CoresAvailable))
		}

		if mem_diff > float64(req.Memory) {
			req.Memory = 0
		} else {
			req.Memory -= uint32(mem_diff)
		}

		if core_diff > float64(req.Cores) {
			req.Cores = 0
		} else {
			req.Cores -= uint32(core_diff)
		}

		node.RunJob(Job{Id: uint(req.Id), CoresNeeded: uint(core_diff), MemoryNeeded: uint(mem_diff), Duration: time.Second * time.Duration(req.Time), Ownership: "Foreign"})
		node.mutex.Unlock()
	}

	return &pb.NodeObject{Id: req.Id, Url: c.URL}
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
	Time            uint
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
