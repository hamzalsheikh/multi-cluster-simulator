package scheduler

import (
	"time"

	trader "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
)

type traderServer struct {
	trader.UnimplementedResourceChannelServer

	currentClusterState trader.ClusterState
}

func NewtraderServer() *traderServer {
	// initialize server
	s := &traderServer{}
	return s
}

func (s *traderServer) Start(params *trader.StartParams, stream trader.ResourceChannel_StartServer) error {
	for {
		// get resource utilization from scheduler
		core, mem := sched.Cluster.GetResourceUtilization()
		s.currentClusterState.CpuUtilization = uint32(core)
		s.currentClusterState.MemoryUtilization = uint32(mem)
		stream.Send(&s.currentClusterState)
		time.Sleep(10 * time.Second)
	}
	// TODO: graceful exit
	// closing the stream
	return nil
}
