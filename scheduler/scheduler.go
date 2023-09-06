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
	Cluster    Cluster
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
	duration     time.Duration
}

type StateType string

const (
	Ready    = StateType("Ready")
	Running  = StateType("Running")
	Waiting  = StateType("Waiting")
	Finished = StateType("Finished")
)

// create an instance of scheduler
var sched = Scheduler{WQueueLock: new(sync.Mutex), RQueueLock: new(sync.Mutex)}

// infinite loop to start scheduling
func (sched Scheduler) Run() {

	switch sched.Policy {
	case FIFO:
		go sched.Fifo()
	}
	// TODO: create channel for policy changes
}

// find a node for job
func (sched Scheduler) ScheduleJob(j Job) error {
	// check for node that satisfies job requirements
	for _, node := range sched.Cluster.Nodes {
		if node.CoresAvailable > j.CoresNeeded && node.MemoryAvailable > j.MemoryNeeded {
			go node.RunJob(j)
			return nil
		}
	}
	return errors.New("not enough resources in cluster")
}

func (sched Scheduler) Fifo() {
	for {
		// check for jobs in waiting queue
		if len(sched.WaitQueue) > 0 {
			sched.WQueueLock.Lock()
			// try scheduling first
			err := sched.ScheduleJob(sched.WaitQueue[0])
			// remove from queue when successful
			if err == nil {
				sched.WaitQueue = sched.WaitQueue[1:]
			}
			sched.WQueueLock.Unlock()
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
			continue
		}
		// wait for new ready chan
		time.Sleep(1 * time.Second)
	}
}
