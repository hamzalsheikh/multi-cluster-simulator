package scheduler

import (
	"errors"
	"sync"
	"time"
)

type Scheduler struct {
	Id            uint
	Name          string
	URL           string
	ReadyQueue    []Job
	WaitQueue     []Job
	LentQueue     []Job // Jobs in my cluster that doesn't belong to me
	BorrowedQueue []Job // Jobs I sent to other clusters
	WQueueLock    *sync.Mutex
	RQueueLock    *sync.Mutex
	LQueueLock    *sync.Mutex
	BQueueLock    *sync.Mutex
	Policy        PolicyType
	Cluster       *Cluster
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
func Run(clt Cluster, URL string) {
	// initialize cluster
	cluster := clt
	for i := 0; i < len(cluster.Nodes); i++ {
		cluster.Nodes[i].mutex = new(sync.Mutex)
		cluster.Nodes[i].RunningJobs = make(map[uint]Job)
	}

	sched.Cluster = &cluster
	sched.URL = URL
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

// find a node for job
func (sched *Scheduler) Lend(j Job) error {
	// check for node that satisfies job requirements
	for _, node := range sched.Cluster.Nodes {
		if node.CoresAvailable > j.CoresNeeded && node.MemoryAvailable > j.MemoryNeeded {
			return nil
		}
	}
	return errors.New("can't lend")
}

func (sched *Scheduler) Fifo() {
	for {
		// check for jobs in waiting queue
		if len(sched.WaitQueue) > 0 {
			sched.WQueueLock.Lock()
			// try scheduling first
			err := sched.ScheduleJob(sched.WaitQueue[0])

			if err == nil {
				// remove from queue when successful
				sched.WaitQueue = sched.WaitQueue[1:]
			} else {
				// not enough resources after waiting in wait queue, try borrowing resources
				// needs to be concurrent, bring back the job to top of queue if failed
				err := sched.BorrowResources(sched.WaitQueue[0])
				if err == nil {
					// TODO: add to borrowedQueue
					sched.BQueueLock.Lock()
					sched.BorrowedQueue = append(sched.BorrowedQueue, sched.WaitQueue[0])
					sched.BQueueLock.Unlock()
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

		// check for jobs in Lent Queue
		if len(sched.LentQueue) > 0 {

			err := sched.ScheduleJob(sched.LentQueue[0])
			sched.LQueueLock.Lock()

			// best effort to schedule lent jobs when
			if err == nil {
				sched.LentQueue = sched.LentQueue[1:]
				// sent job back to borrower
			}

			sched.LQueueLock.Unlock()
			time.Sleep(1 * time.Second)
			continue
		}
		// all queues are empty, create a go routine that wakes me up when that changes
		// wait for new ready chan
		time.Sleep(1 * time.Second)
	}
}
