package trader

import (
	"context"
	"fmt"
	"io"
	"math"
	"time"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
	"google.golang.org/protobuf/types/known/durationpb"
)

func getClusterState(client pb.ResourceChannelClient) {
	trader.Logger.Info().Msg("In getClusterState()")
	stream, err := client.Start(context.Background(), nil)
	if err != nil {
		// TODO: error handling
		fmt.Println(err)
		return
	}
	for {

		_, span := trader.Tracer.Start(context.TODO(), "receiving cluster state")
		state, err := stream.Recv()
		if err == io.EOF {
			trader.Logger.Error().Err(err).Send()
			break
		}

		if err != nil {
			trader.Logger.Error().Err(err).Send()
			return
		}

		if state.GetTotalCpu() != 0 {
			// set whole state
			trader.State.setState(clusterState{TotalMemory: uint(*state.TotalMemory), TotalCore: uint(*state.TotalCpu), MemoryUtilization: state.MemoryUtilization, CoreUtilization: state.CoresUtilization, AverageWaitTime: state.AverageWaitTime})
		} else {
			// set utilization
			trader.State.setUtilization(state)
		}
		span.End()
		trader.Logger.Info().Msgf("received cluster state from scheduler, AverageWaitTime: %v", state.AverageWaitTime)
	}

}

func getVirtualNode(ctx context.Context, client pb.ResourceChannelClient, request *pb.VirtualNodeRequest) (*pb.NodeObject, error) {

	trader.Logger.Info().Msg("in getVirtualNode()")
	trader.Logger.Info().Msgf("request memory %v", request.Memory)
	virtualNode, err := client.ProvideVirtualNode(ctx, request)
	if err != nil {
		trader.Logger.Error().Err(err).Msg("couldn't get get virtual node")
	}
	trader.Logger.Info().Msgf("got virtual node from scheduler %+v", virtualNode)
	return virtualNode, err

}

func sendVirtualNode(ctx context.Context, client pb.ResourceChannelClient, node *pb.NodeObject) {

	trader.Logger.Info().Msg("in sendVirtualNode() to scheduler")
	client.ReceiveVirtualNode(ctx, node)
}

type NodeType string

const (
	smallNode = NodeType("smallNode")
	fastNode  = NodeType("fastNode")
)

func calculateContractRequest(ctx context.Context, client pb.ResourceChannelClient, node NodeType) *pb.ContractRequest {
	trader.Logger.Info().Msg("In calculateContractRequest()")
	ctx, span := trader.Tracer.Start(ctx, "calculating contract request")
	defer span.End()
	jobChan := make(chan *pb.ProvideJobsResponse)

	// go routine requesting jobs from scheduler
	go func(c chan *pb.ProvideJobsResponse, ctx context.Context) {
		trader.Logger.Info().Msg("receiving jobs from scheduler in goroutine")
		stream, err := client.ProvideJobs(ctx, nil)
		if err != nil {
			trader.Logger.Error().Err(err).Send()
			return
		}

		for {
			select {
			case <-ctx.Done():
				close(c)
				return
			default:
				jobs, err := stream.Recv()
				if err == io.EOF {
					close(c)
					trader.Logger.Info().Msg("received all jobs from scheduler")
					return
				}

				if err != nil {
					trader.Logger.Error().Err(err)
					return
				}
				trader.Logger.Info().Msg("sending job batch through jobChan")
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
	trader.Logger.Info().Msg("In calculateFastNode()")
	var contract pb.ContractRequest
	for {
		select {
		case jobs, ok := <-jobChan:
			if !ok {
				trader.Logger.Info().Msg("fast node all level 1 jobs have been accounted for")
				trader.Logger.Info().Msgf("contract id: %v, cores: %v memory %v time %v", contract.Id, contract.Cores, contract.Memory, contract.Time)
				nodeChan <- &contract
				return
			}
			for _, j := range jobs.Jobs {

				trader.Logger.Info().Msgf("job  core %v, memory %v time %v", j.CoresNeeded, j.MemoryNeeded, j.UnixTimeSeconds)
				var newTime time.Duration
				if j.UnixTimeSeconds.AsDuration() > contract.Time.AsDuration() {
					newTime = j.UnixTimeSeconds.AsDuration()
				} else {
					newTime = contract.Time.AsDuration()
				}

				newCores := contract.Cores + j.CoresNeeded
				newMem := contract.Memory + j.MemoryNeeded
				newPrice := newTime.Seconds()*float64(newCores)*float64(trader.MaximimumCoreCost) + float64(trader.MaximimumMemoryCost)*newTime.Seconds()*float64(newMem)
				if newPrice < float64(trader.Budget) || trader.Budget < 0 {
					contract.Cores = newCores
					contract.Memory = newMem
					contract.Time = durationpb.New(newTime)
					contract.Price = float32(newPrice)
				} else {
					trader.Logger.Info().Msg("fast node reached budget")
					nodeChan <- &contract
					return
				}
			}
		case <-time.After(10 * time.Second):

			trader.Logger.Info().Msg("fast node reached timeout")
			nodeChan <- &contract
			return

		}
	}
}

type nodeState struct {
	atTime float64
	cores  int32
	memory int32
}

type jobState struct {
	startTime float64
	endTime   float64
	// total node size
	cores  int32
	memory int32
	time   float64
}

func GetMin(arr []jobState) jobState {
	var min jobState
	var cost float32 = math.MaxFloat32
	for _, j := range arr {
		j_cost := float32(j.cores)*trader.MaximimumCoreCost*float32(j.time) + float32(j.memory)*trader.MaximimumMemoryCost*float32(j.time)
		if j_cost < cost {
			cost = j_cost
			min = j
		}
	}
	return min
}

// this implementation efficiently reduces the size of resources requested given a budget constraint
func calculateSmallNodeSize(nodeChan chan *pb.ContractRequest, jobChan chan *pb.ProvideJobsResponse) {
	trader.Logger.Info().Msg("In calculateSmallNode()")
	var contract pb.ContractRequest
	// tracks changes in scheduling on node
	var atTime []nodeState
	atTime = append(atTime, nodeState{atTime: 0, cores: 0, memory: 0})
	for {
		select {
		case jobs, ok := <-jobChan:
			if !ok {
				trader.Logger.Info().Msg("small node all level 1 jobs have been accounted for")
				nodeChan <- &contract
				return
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
							cores := currState.cores - int32(j.CoresNeeded)
							mem := currState.memory - int32(j.MemoryNeeded)
							if cores < 0 && int32(c.cores) < int32(contract.Cores)-cores {
								c.cores = int32(contract.Cores) - cores
							}
							if mem < 0 && c.memory < int32(contract.Memory)-mem {
								c.memory = int32(contract.Memory) - mem

							}
						}
					}

					// start job atTime
					var startingJob jobState

					cores := currState.cores - int32(j.CoresNeeded)
					mem := currState.memory - int32(j.MemoryNeeded)

					if cores < 0 {
						startingJob.cores = int32(contract.Cores) - cores
					} else {
						startingJob.cores = int32(contract.Cores)
					}
					if mem < 0 {
						startingJob.memory = int32(contract.Memory) - mem
					} else {
						startingJob.memory = int32(contract.Memory)
					}

					startingJob.startTime = t.atTime
					startingJob.endTime = t.atTime + j.UnixTimeSeconds.AsDuration().Seconds()
					if contract.Time.AsDuration().Seconds() < startingJob.endTime {
						startingJob.time = startingJob.endTime
					}
					costArr = append(costArr, startingJob)

					// if the job doesn't fit and we still have a budget, skip job
				}
				min := GetMin(costArr)
				price := float32(min.cores)*trader.MaximimumCoreCost*float32(min.time) + float32(min.memory)*trader.MaximimumMemoryCost*float32(min.time)
				if price < trader.Budget || trader.Budget < 0 {
					contract.Cores = uint32(min.cores)
					contract.Memory = uint32(min.memory)
					contract.Time = durationpb.New(time.Duration(min.time * float64(time.Second)))
					contract.Price = price
				} else {
					trader.Logger.Info().Msg("small node reached budget")
					nodeChan <- &contract
					return
				}
			}
		case <-time.After(10 * time.Second):
			trader.Logger.Info().Msg("small node reached timeout")
			nodeChan <- &contract
			return
		}
	}
}
