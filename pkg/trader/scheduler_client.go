package trader

import (
	"context"
	"fmt"
	"io"
	"time"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
)

func getClusterState(client pb.ResourceChannelClient) {
	ctx := context.Background()
	stream, err := client.Start(ctx, nil)
	if err != nil {
		// TODO: error handling
		fmt.Println(err)
		return
	}
	for {
		state, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			return
		}

		if state.GetTotalCpu() != 0 {
			// set whole state
			trader.State.setState(clusterState{TotalMemory: uint(*state.TotalMemory), TotalCore: uint(*state.TotalCpu), MemoryUtilization: uint(state.MemoryUtilization), CoreUtilization: uint(state.CoresUtilization)})
		} else {
			// set utilization
			trader.State.setUtilization(uint(state.CoresUtilization), uint(state.MemoryUtilization))
		}
	}

}

type nodeState struct {
	atTime int64
	cores  uint32
	memory uint32
}

type jobState struct {
	startTime int64
	endTime   int64
	// total node size
	cores  uint32
	memory uint32
	time   int64
}

func GetMin(arr []jobState) jobState {
	var min jobState
	var cost uint32 = ^uint32(0)
	for _, j := range arr {
		j_cost := j.cores*trader.MaximimumCoreCost*uint32(j.time) + j.memory*trader.MaximimumMemoryCost*uint32(j.time)
		if j_cost < cost {
			cost = j_cost
			min = j
		}
	}
	return min
}

type NodeType string

const (
	smallNode = NodeType("smallNode")
	fastNode  = NodeType("fastNode")
)

func calculateContractRequest(client pb.ResourceChannelClient, node NodeType) *pb.ContractRequest {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobChan := make(chan *pb.ProvideJobsResponse)

	// go routine requesting jobs from scheduler
	go func(c chan *pb.ProvideJobsResponse, ctx context.Context) {
		stream, err := client.ProvideJobs(ctx, nil)
		if err != nil {
			fmt.Println(err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
				jobs, err := stream.Recv()
				if err == io.EOF {
					break
				}

				if err != nil {
					fmt.Println(err)
					return
				}
				c <- jobs
			}
		}
	}(jobChan, ctx)

	// Greedy algorithm to populate node size
	requestChan := make(chan *pb.ContractRequest)

	switch node {
	case smallNode:
		go calculateSmallNodeSize(requestChan, jobChan)
	case fastNode:
		go calculateFastNodeSize(requestChan, jobChan)
	}

	return <-requestChan
}

// calculate a node size with all the jobs starting at time 0 for faster execution
func calculateFastNodeSize(nodeChan chan *pb.ContractRequest, jobChan chan *pb.ProvideJobsResponse) {
	var contract pb.ContractRequest
	for {
		select {
		case jobs, ok := <-jobChan:
			if !ok {
				nodeChan <- &contract
			}
			for _, j := range jobs.Jobs {
				var newTime int64
				if j.UnixTimeSeconds > contract.Time {
					newTime = j.UnixTimeSeconds
				} else {
					newTime = contract.Time
				}

				newCores := contract.Cores + j.CoresNeeded
				newMem := contract.Memory + j.MemoryNeeded
				newPrice := newTime*int64(newCores)*int64(trader.MaximimumCoreCost) + int64(trader.MaximimumMemoryCost)*newTime*int64(newMem)
				if newPrice < int64(trader.Budget) {
					contract.Cores = newCores
					contract.Memory = newMem
					contract.Time = newTime
					contract.Price = uint32(newPrice)
				} else {
					nodeChan <- &contract
					return
				}
			}
		case <-time.After(10 * time.Second):
			nodeChan <- &contract
			return

		}
	}
}

// this implementation efficiently reduces the size of resources requested given a budget constraint
func calculateSmallNodeSize(nodeChan chan *pb.ContractRequest, jobChan chan *pb.ProvideJobsResponse) {
	var contract pb.ContractRequest
	// tracks changes in scheduling on node
	var atTime []nodeState
	atTime = append(atTime, nodeState{atTime: 0, cores: 0, memory: 0})
	for {
		select {
		case jobs, ok := <-jobChan:
			if !ok {
				nodeChan <- &contract
			}
			for _, j := range jobs.Jobs {

				// the costArray measure the cost of starting the job at each time and pops to a minheap when the job is fully scheduled
				// minheap is sorted by cost.
				var costArr []jobState // can be ordered map
				var currState nodeState
				for _, t := range atTime {

					currState.cores += t.cores
					currState.memory += t.memory
					// loop over costArray, to see if any starttime ended
					// check for max of each job start time
					// In cost array, atTime is the potential start time
					// time, core, memory is the node total will be if the job is scheduled at the atTime
					for _, c := range costArr {
						// job cost established
						if c.endTime > t.atTime {
							// max total core should be reported for cost evaluation
							cores := currState.cores - j.CoresNeeded
							mem := currState.memory - j.MemoryNeeded
							if cores < 0 && c.cores < contract.Cores-cores {
								c.cores = contract.Cores - cores
							}
							if mem < 0 && c.memory < contract.Memory-mem {
								c.memory = contract.Memory - mem

							}
						}
					}

					// start job atTime
					var startingJob jobState

					cores := currState.cores - j.CoresNeeded
					mem := currState.memory - j.MemoryNeeded

					if cores < 0 {
						startingJob.cores = contract.Cores - cores
					} else {
						startingJob.cores = contract.Cores
					}
					if mem < 0 {
						startingJob.memory = contract.Memory - mem
					} else {
						startingJob.memory = contract.Memory
					}

					startingJob.startTime = t.atTime
					startingJob.endTime = t.atTime + j.UnixTimeSeconds
					if contract.Time < startingJob.endTime {
						startingJob.time = startingJob.endTime
					}
					costArr = append(costArr, startingJob)

					// if the job doesn't fit and we still have a budget, skip job
				}
				min := GetMin(costArr)
				price := min.cores*trader.MaximimumCoreCost*uint32(min.time) + min.memory*trader.MaximimumMemoryCost*uint32(min.time)
				if price < trader.Budget {
					contract.Cores = min.cores
					contract.Memory = min.memory
					contract.Time = min.time
					contract.Price = price
				}
			}
		case <-time.After(10 * time.Second):
			nodeChan <- &contract
			return
		}
	}
}

func getVirtualNode(client pb.ResourceChannelClient, request *pb.VirtualNodeRequest) (*pb.NodeObject, error) {
	ctx := context.Background()

	virtualNode, err := client.ProvideVirtualNode(ctx, request)

	return virtualNode, err

}

func sendVirtualNode(client pb.ResourceChannelClient, node *pb.NodeObject) {
	ctx := context.Background()
	client.ReceiveVirtualNode(ctx, node)
}
