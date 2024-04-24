package scheduler

import (
	"context"
	"time"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
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
	for {
		var currentClusterState pb.ClusterState
		var core, mem uint
		if s.ClusterChange {

			core, mem = sched.Cluster.GetTotalResources()
			*currentClusterState.TotalCpu, *currentClusterState.TotalMemory = uint32(core), uint32(mem)
			s.ClusterChange = false
		}
		// get resource utilization from scheduler
		core, mem = sched.Cluster.GetResourceUtilization()
		*&currentClusterState.CoresUtilization = uint32(core)
		currentClusterState.MemoryUtilization = uint32(mem)
		stream.Send(&currentClusterState)
		time.Sleep(10 * time.Second)
	}
	// TODO: graceful exit
	// closing the stream
	return nil
}

func (s *traderServer) ReceiveVirtualNode(ctx context.Context, node *pb.NodeObject) (*pb.VirtualNodeResponse, error) {

}

func (s *traderServer) ProvideVirtualNode(ctx context.Context, req *pb.VirtualNodeRequest) (*pb.NodeObject, error) {

}

func (s *traderServer) ProvideJobs(ctx context.Context, stream pb.ResourceChannel_ProvideJobsServer) error {
	// Get level one jobs
	l1 := sched.GetLevel1()
	BATCH := 20
	for i := 0; i < len(l1); i += BATCH {

		// parse per batch
		batch := make([]*pb.Job, BATCH)

		for j := 0; j < len(l1) || j < BATCH; j++ {
			// will this create new memory allocation at each step?
			batch[j] = &pb.Job{
				CoresNeeded:     uint32(l1[i+j].CoresNeeded),
				MemoryNeeded:    uint32(l1[i+j].MemoryNeeded),
				UnixTimeSeconds: int64(l1[i+j].Duration),
			}
		}
		stream.Send(&pb.ProvideJobsResponse{Jobs: batch})
	}
	return nil
}
