package trader

import (
	"sync"

	"go.opentelemetry.io/otel/trace"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
)

type Trader struct {
	Name               string
	URL                string
	State              clusterState
	SchedulerURL       string
	Tracer             trace.Tracer
	SchedulerRPCClient pb.ResourceChannelClient
}

type clusterState struct {
	MemoryUtilization uint
	CoreUtilization   uint
	mutex             *sync.Mutex
}

var trader Trader

func Run(schedURL string, URL string, schedClient pb.ResourceChannelClient) {
	trader.SchedulerURL = schedURL
	trader.URL = URL
	trader.SchedulerRPCClient = schedClient

	//trader.initialize_tracer()
	trader.newTrader()

	getClusterState(trader.SchedulerRPCClient)
}

func SetTracer(t trace.Tracer) {
	trader.Tracer = t
}

func (t *Trader) newTrader() {
	// request cluster information & this can include more information
	// in the future
	// key exchange ?
	t.State.mutex = new(sync.Mutex)
}

// policy logic goes here
