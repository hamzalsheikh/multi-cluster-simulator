package scheduler

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
	"go.opentelemetry.io/otel/metric"
	api "go.opentelemetry.io/otel/metric"
)

type Cluster struct {
	Id                uint
	Nodes             []*Node
	nodesMutex        *sync.Mutex
	URL               string
	TotalMemory       uint32
	TotalCore         uint32
	MemoryUtilization float32
	CoreUtilization   float32
	resourceMutex     *sync.Mutex
}

func InitCluster(clt Cluster) (*Cluster, error) {
	cluster := clt
	for i := 0; i < len(cluster.Nodes); i++ {
		cluster.Nodes[i].mutex = new(sync.Mutex)
		cluster.Nodes[i].RunningJobs = make(map[uint]Job)
	}
	cluster.resourceMutex = new(sync.Mutex)
	cluster.nodesMutex = new(sync.Mutex)
	cluster.SetTotalResources()

	go cluster.updateUtilization(3 * time.Second)
	cluster.recordUtilization()

	return &cluster, nil
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

func (c *Cluster) SetResourceUtilization() {
	c.resourceMutex.Lock()
	defer c.resourceMutex.Unlock()

	c.CoreUtilization = 0
	c.MemoryUtilization = 0
	for _, node := range c.Nodes {
		node.mutex.Lock()
		c.CoreUtilization += (float32(node.Cores) - float32(node.CoresAvailable))
		c.MemoryUtilization += (float32(node.Memory) - float32(node.MemoryAvailable))
		node.mutex.Unlock()
	}
	total_core, total_mem := c.GetTotalResources()

	c.CoreUtilization, c.MemoryUtilization = c.CoreUtilization/float32(total_core), c.MemoryUtilization/float32(total_mem)
	sched.logger.Info().Msgf("core utilization: %v, memory utilization: %v", c.CoreUtilization, c.MemoryUtilization)
}

func (c *Cluster) GetResourceUtilization() (float32, float32) {
	c.resourceMutex.Lock()
	defer c.resourceMutex.Unlock()

	return c.CoreUtilization, c.MemoryUtilization
}

func (c *Cluster) recordUtilization() {

	gauge, err := sched.meter.Float64ObservableGauge(
		os.Getenv("SERVICE_NAME")+"_memory_utilization",
		api.WithUnit("%"),
		api.WithDescription("reports memory utilization between 0 and 1"),
	)

	if err != nil {
		sched.logger.Info().Msgf("error creating memory utilization gauge: %v", err)
	}

	_, err = sched.meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			c.resourceMutex.Lock()
			defer c.resourceMutex.Unlock()

			o.ObserveFloat64(gauge, float64(c.MemoryUtilization))
			return nil
		},
		gauge,
	)

	if err != nil {
		sched.logger.Info().Msg("error registring core utilization callback")
	}

	sched.meter.Float64ObservableGauge(
		os.Getenv("SERVICE_NAME")+"_core_utilization",
		metric.WithFloat64Callback(
			func(ctx context.Context, fo metric.Float64Observer) error {
				c.resourceMutex.Lock()
				defer c.resourceMutex.Unlock()

				fo.Observe(float64(c.CoreUtilization))
				return nil
			},
		),
	)
}

func (c *Cluster) updateUtilization(period time.Duration) {
	for {
		// update utilization
		c.SetResourceUtilization()
		time.Sleep(period)
	}
}

func (c *Cluster) AddVirtualNode(ctx context.Context, node *pb.NodeObject) {
	sched.logger.Info().Msgf("in AddVirtualNode()\n node: %+v")
	var n Node
	n.Type = "Virtual"
	n.Id = uint(len(c.Nodes))
	n.Cores = uint(node.Cores)
	n.Memory = uint(node.Memory)
	n.CoresAvailable = uint(node.Cores)
	n.MemoryAvailable = uint(node.Memory)
	n.Time = node.Time.AsDuration()
	n.URL = node.Url
	n.mutex = new(sync.Mutex)
	n.RunningJobs = make(map[uint]Job)
	sched.logger.Info().Msgf("created node %+v", n)
	c.Nodes = append(c.Nodes, &n)

	go c.RemoveVirtualNode(ctx, n)

}

func (c *Cluster) RemoveVirtualNode(ctx context.Context, n Node) {
	time.Sleep(n.Time)
	sched.logger.Info().Msg("in RemoveVirtualNode()")
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	for i, node := range c.Nodes {
		if node.Id == n.Id && node.Type == "Virtual" {
			// order is not important
			c.Nodes[i] = c.Nodes[len(c.Nodes)-1]
			c.Nodes = c.Nodes[:len(c.Nodes)-1]
			sched.logger.Info().Msgf("virtual Node %v successfully removed", n.Id)
		}
	}
}

func (c *Cluster) AllocateVirtualNodeResources(req *pb.VirtualNodeRequest) error {
	sched.logger.Info().Msg("in AllocateVirtualNodeResources()")
	for _, node := range c.Nodes {
		if req.Memory == 0 && req.Cores == 0 {
			break
		}
		node.mutex.Lock()
		var cores uint
		var memory uint
		if node.CoresAvailable >= uint(req.Cores) {
			cores = uint(req.Cores)
			req.Cores = 0
		} else {
			cores = node.CoresAvailable
			req.Cores -= uint32(node.CoresAvailable)
		}

		if node.MemoryAvailable >= uint(req.Memory) {
			memory = uint(req.Memory)
			req.Memory = 0
		} else {
			memory = node.MemoryAvailable
			req.Memory -= uint32(node.MemoryAvailable)
		}

		sched.logger.Info().Msgf("req.Cores: %v req.Mem %v\n", req.Cores, req.Memory)
		node.mutex.Unlock()
		err := node.RunJob(Job{Id: uint(req.Id), CoresNeeded: cores, MemoryNeeded: memory, Duration: req.Time.AsDuration(), Ownership: "External"})
		if err != nil {
			sched.logger.Error().Err(err).Msgf("Error running job on node %v", node.Id)
		}
	}
	if req.Cores > 0 || req.Memory > 0 {
		// generally speaking this should not happen
		sched.logger.Info().Msgf("the request is not fully accounted for: cores %v or memory %v", req.Cores, req.Memory)
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
	Time            time.Duration
	mutex           *sync.Mutex
}

// simulate running a job
func (n *Node) RunJob(j Job) error {

	// update node state
	n.mutex.Lock()
	n.RunningJobs[j.Id] = j
	n.CoresAvailable -= j.CoresNeeded
	n.MemoryAvailable -= j.MemoryNeeded
	n.mutex.Unlock()
	go n.FinishJob(j)
	return nil
}

func (n *Node) FinishJob(j Job) error {
	time.Sleep(j.Duration)

	n.mutex.Lock()
	delete(n.RunningJobs, j.Id)
	n.CoresAvailable += j.CoresNeeded
	n.MemoryAvailable += j.MemoryNeeded
	n.mutex.Unlock()
	// inform scheduler that you're done
	sched.JobFinished(j)
	return nil
}
