package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	api "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Scheduler struct {
	Id                  uint
	Name                string
	URL                 string
	ReadyQueue          []Job
	WaitQueue           []Job
	LentQueue           []Job // Jobs in my cluster that doesn't belong to me
	BorrowedQueue       []Job // Jobs I sent to other clusters
	WQueueLock          *sync.Mutex
	RQueueLock          *sync.Mutex
	LQueueLock          *sync.Mutex
	BQueueLock          *sync.Mutex
	Level0              []Job // only schedule in cluster
	Level1              []Job // allow scheduling in foreign cluster
	L0Lock              *sync.Mutex
	L1Lock              *sync.Mutex
	SchedulingAlgorithm SchedulingType
	Policy              Policy
	Cluster             *Cluster
	tracer              trace.Tracer
	meter               api.Meter
	WaitTime            *WaitTime
}

type SchedulingType string

const (
	FIFO  = SchedulingType("FIFO")
	DELAY = SchedulingType("DELAY")
)

// Wait time in Milliseconds
type WaitTime struct {
	Lock      *sync.Mutex
	Average   float64
	Total     int64
	JobsCount int64
	JobsMap   map[uint]int64
}

func (w *WaitTime) GetAverage() float64 {
	w.Lock.Lock()
	defer w.Lock.Unlock()
	if w.JobsCount != 0 {
		return float64(w.Total) / float64(w.JobsCount)
	}
	return 0
}

type Job struct {
	Id           uint
	MemoryNeeded uint
	CoresNeeded  uint
	State        StateType
	Duration     time.Duration
	WaitTime     time.Time
	Ownership    string
}

type Policy struct {
	MaxWaitTime time.Duration
}

type StateType string

const (
	READY    = StateType("Ready")
	RUNNING  = StateType("Running")
	WAITING  = StateType("Waiting")
	Finished = StateType("Finished")
)

func SetTracer(t trace.Tracer) {
	sched.tracer = t
}

func SetMeter(m api.Meter) {
	sched.meter = m
}

// infinite loop to start scheduling
func Run(clt Cluster, URL string) {
	// initialize cluster
	cluster := clt
	for i := 0; i < len(cluster.Nodes); i++ {
		cluster.Nodes[i].mutex = new(sync.Mutex)
		cluster.Nodes[i].RunningJobs = make(map[uint]Job)
	}
	cluster.resourceMutex = new(sync.Mutex)
	cluster.SetTotalResources()

	RunMetrics()

	sched.Cluster = &cluster
	sched.URL = URL
	sched.Policy.MaxWaitTime = 10 * time.Second
	sched.SchedulingAlgorithm = DELAY
	switch sched.SchedulingAlgorithm {
	case FIFO:
		go sched.Fifo()
	case DELAY:
		go sched.Delay()
	}
	// TODO: create channel for policy changes
}

// find a node for job
func (sched *Scheduler) ScheduleJob(j Job) error {
	// check for node that satisfies job requirements
	for _, node := range sched.Cluster.Nodes {
		node.mutex.Lock()
		if node.CoresAvailable > j.CoresNeeded && node.MemoryAvailable > j.MemoryNeeded {
			node.mutex.Unlock()
			go node.RunJob(j)
			return nil
		}
		node.mutex.Unlock()
	}
	return errors.New("not enough resources in cluster")
}

func (sched *Scheduler) ScheduleJobsOnVirtual(node *Node) {
	// Can introduce network delay simulation
	sched.L1Lock.Lock()
	for _, j := range sched.Level1 {
		node.mutex.Lock()
		if node.CoresAvailable > j.CoresNeeded && node.MemoryAvailable > j.MemoryNeeded {
			go node.RunJob(j)
		}
		node.mutex.Unlock()
		// wait for job to be scheduled
		time.Sleep(20 * time.Millisecond)
	}
	sched.L1Lock.Unlock()
}

func (sched *Scheduler) JobFinished(j Job) {
	if j.Ownership == "" {
		// my job so remove from state queue
		if j.State == WAITING {
			sched.WQueueLock.Lock()
			for i := 0; i < len(sched.WaitQueue); i++ {
				if j == sched.WaitQueue[i] {
					sched.WaitQueue = append(sched.WaitQueue[:i], sched.WaitQueue[i+1:]...)
				}
			}
			sched.WQueueLock.Unlock()
		} else if j.State == READY {
			sched.RQueueLock.Lock()
			for i := 0; i < len(sched.ReadyQueue); i++ {
				if j == sched.ReadyQueue[i] {
					sched.ReadyQueue = append(sched.ReadyQueue[:i], sched.ReadyQueue[i+1:]...)
				}
			}
			sched.RQueueLock.Unlock()
		}
		return
	}

	// job is for some other cluster
	sched.LQueueLock.Lock()
	for i := 0; i < len(sched.LentQueue); i++ {
		if j == sched.LentQueue[i] {
			sched.LentQueue = append(sched.LentQueue[:i], sched.LentQueue[i+1:]...)
		}
	}
	sched.LQueueLock.Unlock()

	sched.ReturnToBorrower(j)
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

func (sched *Scheduler) GetLevel1() []Job {
	sched.L1Lock.Lock()
	defer sched.L1Lock.Unlock()

	var l1 []Job
	copy(l1, sched.Level1)
	return l1
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
				fmt.Printf("sent job %v to node from wait queue\n", sched.WaitQueue[0].Id)

			} else {
				// not enough resources after waiting in wait queue, try borrowing resources
				// needs to be concurrent, bring back the job to top of queue if failed
				fmt.Printf("not enough resources for job: %v\nTrying to borrow\n", sched.WaitQueue[0].Id)

				err := sched.BorrowResources(sched.WaitQueue[0])
				if err == nil {
					fmt.Printf("successfully borrowed resources for j %v\n", sched.WaitQueue[0].Id)

					// TODO: add to borrowedQueue
					sched.BQueueLock.Lock()
					sched.BorrowedQueue = append(sched.BorrowedQueue, sched.WaitQueue[0])
					sched.BQueueLock.Unlock()
					sched.WaitQueue = sched.WaitQueue[1:]
					// start a go routine that tracks the time spent at lender, canceling exchange if time limit exceeded
				}
				//fmt.Printf("couldn't borrow resources for job: %+v\n", sched.WaitQueue[0])

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
			fmt.Printf("sent job %v to node from ready queue\n", j.Id)
			if err != nil {
				sched.WQueueLock.Lock()
				j.State = WAITING
				sched.WaitQueue = append(sched.WaitQueue, j)
				sched.WQueueLock.Unlock()
				fmt.Printf("sent job %v to wait queue\n", j.Id)

			}
			// time.Sleep(1 * time.Second)
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

func (sched *Scheduler) Delay() {

	counter, _ := sched.meter.Int64UpDownCounter("jobs_in_queue")
	waitTimeMeter, _ := sched.meter.Int64Histogram("waitTime")
	for {
		sched.L1Lock.Lock()
		if len(sched.Level1) > 0 {
			// try to scheduler job
			for i := 0; i < len(sched.Level1); i++ {

				err := sched.ScheduleJob(sched.Level1[i])
				// check if job got scheduled
				sched.WaitTime.Lock.Lock()
				sched.WaitTime.Total -= sched.WaitTime.JobsMap[sched.Level1[i].Id]
				sched.WaitTime.JobsMap[sched.Level1[i].Id] = time.Since(sched.Level1[i].WaitTime).Milliseconds()
				sched.WaitTime.Total += sched.WaitTime.JobsMap[sched.Level1[i].Id]
				if err == nil {
					// Send telemetry
					waitTimeMeter.Record(context.Background(), sched.Level1[i].WaitTime.UnixMilli())
					// Update cluster wait time and delete job from map
					delete(sched.WaitTime.JobsMap, sched.Level1[i].Id)
					sched.WaitTime.Lock.Unlock()
					fmt.Printf("scheduled job %v from level 1\n", sched.Level1[i].Id)
					// remove job from level1 queue
					sched.Level1 = append(sched.Level1[:i], sched.Level1[i+1:]...)
					counter.Add(context.Background(), -1)

				} else {

					fmt.Printf("couldn't schedule job %v from level 1\n", sched.Level1[i].Id)
				}
				sched.WaitTime.Lock.Unlock()
			}
		}
		sched.L1Lock.Unlock()

		// level 1 is empty
		sched.L0Lock.Lock()
		if len(sched.Level0) > 0 {

			err := sched.ScheduleJob(sched.Level0[0])
			// check if job got scheduled

			sched.WaitTime.Lock.Lock()
			sched.WaitTime.Total -= sched.WaitTime.JobsMap[sched.Level0[0].Id]
			sched.WaitTime.JobsMap[sched.Level0[0].Id] = time.Since(sched.Level0[0].WaitTime).Milliseconds()
			sched.WaitTime.Total += sched.WaitTime.JobsMap[sched.Level0[0].Id]
			if err == nil {
				// remove job from level1 queue
				fmt.Printf("scheduled job %v from level 0\n", sched.Level0[0].Id)

				delete(sched.WaitTime.JobsMap, sched.Level0[0].Id)
				sched.Level0 = sched.Level0[1:]
				counter.Add(context.Background(), -1)
			} else {
				fmt.Printf("couldn't schedule job %v in level 0, wait time: %v\n", sched.Level0[0].Id, sched.Level0[0].WaitTime)

				// check if time exceeded to put in Level1
				if time.Since(sched.Level0[0].WaitTime) >= sched.Policy.MaxWaitTime {
					sched.L1Lock.Lock()

					fmt.Printf("moved job %v from level 0 to level 1", sched.Level0[0].Id)
					sched.Level1 = append(sched.Level1, sched.Level0[0])
					sched.Level0 = sched.Level0[1:]
					sched.L1Lock.Unlock()
				}

			}

			sched.WaitTime.Lock.Unlock()
		}
		sched.L0Lock.Unlock()
		time.Sleep(1 * time.Second)
	}
}
