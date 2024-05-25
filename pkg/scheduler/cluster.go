package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
)

type Cluster struct {
	Id                uint
	Nodes             []*Node
	URL               string
	TotalMemory       uint32
	TotalCore         uint32
	MemoryUtilization float32
	CoreUtilization   float32
	resourceMutex     *sync.Mutex
}

func (c *Cluster) SetTotalResources() error {
	// function part of cluster initialization

	c.resourceMutex.Lock()
	defer c.resourceMutex.Unlock()

	c.TotalCore = 0
	c.TotalMemory = 0
	for _, node := range c.Nodes {
		c.TotalCore += uint32(node.Cores)
		c.TotalMemory += uint32(node.Memory)
	}

	return nil
}

func (c *Cluster) GetTotalResources() (uint32, uint32) {
	return c.TotalCore, c.TotalMemory
}

func (c *Cluster) GetResourceUtilization() (float32, float32) {
	c.resourceMutex.Lock()
	defer c.resourceMutex.Unlock()

	fmt.Printf("in GetResourceUtilization()\n")
	c.CoreUtilization = 0
	c.MemoryUtilization = 0
	for _, node := range c.Nodes {
		node.mutex.Lock()
		c.CoreUtilization += (float32(node.Cores) - float32(node.CoresAvailable))
		c.MemoryUtilization += (float32(node.Memory) - float32(node.MemoryAvailable))
		node.mutex.Unlock()
	}
	total_core, total_mem := c.GetTotalResources()
	fmt.Printf("util %v %v\n", c.CoreUtilization, c.MemoryUtilization)
	fmt.Printf("%v %v\n", total_core, total_mem)
	return c.CoreUtilization / float32(total_core), c.MemoryUtilization / float32(total_mem)
}

func (c *Cluster) AddVirtualNode(ctx context.Context, node *pb.NodeObject) {
	sched.logger.Info().Msg("in AddVirtualNode()")
	var n Node
	n.Type = "Virtual"
	n.CoresAvailable = uint(node.Cores)
	n.MemoryAvailable = uint(node.Memory)
	n.Time = uint(node.Time)
	n.URL = node.Url
	n.mutex = new(sync.Mutex)
	sched.logger.Info().Msgf("created node %+v", n)
	//c.Nodes = append(c.Nodes, n)
	sched.ScheduleJobsOnVirtual(ctx, &n)
}

func (c *Cluster) AllocateVirtualNodeResources(req *pb.VirtualNodeRequest) error {
	sched.logger.Info().Msg("in AllocateVirtualNodeResources()")
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
	if req.Cores > 0 || req.Memory > 0 {
		// generally speaking this should not happen
		return errors.New("couldn't schedule enough resources")
	}
	sched.logger.Info().Msg("successfully allocated resources for trade")
	return nil
}

type Node struct {
	Id              uint
	Type            string
	URL             string
	Memory          uint
	Cores           uint
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
	fmt.Printf("node %v running job %v cores: %v mem: %v \n", n.Id, j.Id, n.CoresAvailable, n.MemoryAvailable)

	time.Sleep(j.Duration)

	n.mutex.Lock()
	delete(n.RunningJobs, j.Id)
	n.CoresAvailable += j.CoresNeeded
	n.MemoryAvailable += j.MemoryNeeded
	n.mutex.Unlock()
	fmt.Printf("node %v finished job %v cores: %v mem: %v\n", n.Id, j.Id, n.CoresAvailable, n.MemoryAvailable)
	// inform scheduler that you're done
	// should be a http request, but call the function directly for now
	sched.JobFinished(j)
}
