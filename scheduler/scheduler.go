package scheduler

import (
	"errors"
	"sync"
	"time"
)

type Scheduler struct {
	Id         uint
	Name       string
	ReadyQueue []Job
	WaitQueue  []Job
	WQueueLock *sync.Mutex
	RQueueLock *sync.Mutex
	Policy     PolicyType
	Cluster    *Cluster
}

type PolicyType string

const (
	FIFO = PolicyType("FIFO")
)

type Job struct {
	Id           uint
	MemoryNeeded uint
	CoresNeeded  uint
	State        StateType
	Duration     time.Duration
	Ownership    string
}

type StateType string

const (
	READY    = StateType("Ready")
	RUNNING  = StateType("Running")
	WAITING  = StateType("Waiting")
	Finished = StateType("Finished")
)

// infinite loop to start scheduling
func Run(clt Cluster) {
	// initialize cluster
	cluster := clt
	for i := 0; i < len(cluster.Nodes); i++ {
		cluster.Nodes[i].mutex = new(sync.Mutex)
		cluster.Nodes[i].RunningJobs = make(map[uint]Job)
	}

	sched.Cluster = &cluster

	switch sched.Policy {
	case FIFO:
		go sched.Fifo()
	}
	// TODO: create channel for policy changes
}

// find a node for job
func (sched *Scheduler) ScheduleJob(j Job) error {
	// check for node that satisfies job requirements
	for _, node := range sched.Cluster.Nodes {
		if node.CoresAvailable > j.CoresNeeded && node.MemoryAvailable > j.MemoryNeeded {
			go node.RunJob(j)
			return nil
		}
	}
	return errors.New("not enough resources in cluster")
}

func (sched *Scheduler) Fifo() {
	for {
		// check for jobs in waiting queue
		if len(sched.WaitQueue) > 0 {
			sched.WQueueLock.Lock()
			// try scheduling first
			err := sched.ScheduleJob(sched.WaitQueue[0])
			// remove from queue when successful
			if err == nil {
				sched.WaitQueue = sched.WaitQueue[1:]
			} else {
				// not enough resources after waiting in wait queue, try borrowing resources
				// needs to be concurrent, bring back the job to top of queue if failed
				sched.BorrowResources(sched.WaitQueue[0])
				if err == nil {
					sched.WaitQueue = sched.WaitQueue[1:]
				}
			}

			sched.WQueueLock.Unlock()
			time.Sleep(1 * time.Second)
			continue
		}

		// check for jobs in ready queue
		if len(sched.ReadyQueue) > 0 {
			sched.RQueueLock.Lock()
			err := sched.ScheduleJob(sched.ReadyQueue[0])
			j := sched.ReadyQueue[0]
			sched.ReadyQueue = sched.ReadyQueue[1:]

			sched.RQueueLock.Unlock()

			if err != nil {
				sched.WQueueLock.Lock()
				sched.WaitQueue = append(sched.WaitQueue, j)
				sched.WQueueLock.Unlock()
			}
			time.Sleep(1 * time.Second)
			continue
		}
		// wait for new ready chan
		time.Sleep(1 * time.Second)
	}
}
