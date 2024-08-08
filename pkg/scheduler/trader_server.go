package scheduler

import (
	"context"
	"time"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
	"google.golang.org/protobuf/types/known/durationpb"
)

type traderServer struct {
	pb.UnimplementedResourceChannelServer
	currentClusterState pb.ClusterState
	ClusterChange       bool // change bool whenever a change other than utilization occurs
}

func NewtraderServer() *traderServer {
	// initialize server
	s := &traderServer{}
	s.ClusterChange = true
	return s
}

func (s *traderServer) Start(params *pb.StartParams, stream pb.ResourceChannel_StartServer) error {
	sched.traderConnected = true
	for {
		var currentClusterState pb.ClusterState
		var core, mem uint32
		if s.ClusterChange {

			core, mem = sched.Cluster.GetTotalResources()
			core_32, mem_32 := uint32(core), uint32(mem)
			currentClusterState.TotalCpu, currentClusterState.TotalMemory = &core_32, &mem_32
			s.ClusterChange = false
		}
		// get resource utilization from scheduler
		core_util, mem_util := sched.Cluster.GetResourceUtilization()
		currentClusterState.CoresUtilization = core_util
		currentClusterState.MemoryUtilization = mem_util
		currentClusterState.AverageWaitTime = sched.WaitTime.GetCurrentAverage()
		sched.logger.Info().Msgf("In scheduler util: core %v mem %v wt %v", currentClusterState.CoresUtilization, currentClusterState.MemoryUtilization, currentClusterState.AverageWaitTime)
		stream.Send(&currentClusterState)
		time.Sleep(5 * time.Second)
	}
	// TODO: graceful exit
	// closing the stream
	return nil
}

func (s *traderServer) ReceiveVirtualNode(ctx context.Context, node *pb.NodeObject) (*pb.VirtualNodeResponse, error) {
	sched.logger.Info().Msg("in ReceiveVirtualNode()")

	sched.logger.Info().Msgf("received node %+v", node.Cores)
	sched.Cluster.AddVirtualNode(ctx, node)
	return nil, nil
}

func (s *traderServer) ProvideVirtualNode(ctx context.Context, req *pb.VirtualNodeRequest) (*pb.NodeObject, error) {
	node := pb.NodeObject{Id: req.Id, Memory: req.Memory, Cores: req.Cores, Time: req.Time}
	sched.logger.Info().Msgf("in ProvideVirtualNode() %v ", req.Memory)
	err := sched.Cluster.AllocateVirtualNodeResources(req)
	if err != nil {
		sched.logger.Error().Err(err).Msg("couldn't allocate resources on scheduler")
		return nil, err
	}
	sched.logger.Info().Msgf("allocated resource for virtual node core %v mem %v time %v", node.Cores, node.Memory, node.Time)
	return &node, nil
}

func (s *traderServer) ProvideJobs(req *pb.ProvideJobsRequest, stream pb.ResourceChannel_ProvideJobsServer) error {
	// Get level one jobs
	sched.logger.Info().Msg("In Providejobs()")
	l1 := sched.GetLevel1()

	sched.logger.Info().Msgf("l1 len %v", len(l1))
	BATCH := 20
	for i := 0; i < len(l1); i += BATCH {

		// parse per batch
		batch := make([]*pb.Job, BATCH)

		for j := 0; j+i < len(l1) && j < BATCH; j++ {
			// will this create new memory allocation at each step?
			sched.logger.Info().Msgf("Adding job %v to batch", l1[i+j].Id)
			batch[j] = &pb.Job{
				CoresNeeded:     uint32(l1[i+j].CoresNeeded),
				MemoryNeeded:    uint32(l1[i+j].MemoryNeeded),
				UnixTimeSeconds: durationpb.New(l1[i+j].Duration),
			}
		}
		stream.Send(&pb.ProvideJobsResponse{Jobs: batch})
	}
	sched.logger.Info().Msg("All jobs have been sent")
	return nil
}
