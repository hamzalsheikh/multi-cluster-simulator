package trader

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/registry"
	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
)

type Trader struct {
	Name            string
	URL             string
	State           clusterState
	ApprovePolicy   approvePolicy
	RequestPolicy   requestPolicy
	SchedulerURL    string
	Tracer          trace.Tracer
	SchedulerClient pb.ResourceChannelClient   // gRPC client
	TraderClients   map[string]pb.TraderClient // gRPC client
}

func (t *Trader) newTrader() {
	// request cluster information & this can include more information
	// in the future
	// key exchange ?
	t.State.mutex = new(sync.Mutex)
}

type clusterState struct {
	TotalMemory       uint
	TotalCore         uint
	MemoryUtilization uint
	CoreUtilization   uint
	mutex             *sync.Mutex
}

func (c *clusterState) setState(state clusterState) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.TotalMemory, c.TotalCore = state.TotalMemory, state.TotalCore
	c.CoreUtilization, c.MemoryUtilization = state.CoreUtilization, state.MemoryUtilization

}

func (c *clusterState) setUtilization(core, mem uint) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.CoreUtilization, c.MemoryUtilization = core, mem
}

func (c *clusterState) getState() clusterState {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return clusterState{
		TotalMemory:       c.TotalMemory,
		TotalCore:         c.TotalCore,
		MemoryUtilization: c.MemoryUtilization,
		CoreUtilization:   c.CoreUtilization,
	}
}

type approvePolicy struct {
	MemoryThreshold        uint
	CoreThreshold          uint
	MinimumCoreIncentive   uint // per second
	MinimumMemoryIncentive uint // per second
}

type requestPolicy struct {
	MemoryMax           uint32
	CoreMax             uint32
	MaximimumCoreCost   uint32 // per second
	MaximimumMemoryCost uint32 // per second
	Budget              uint32
}

func (t *Trader) ApproveTrade(contract *pb.ContractRequest) bool {
	// check if memory
	clusterState := t.State.getState()
	if clusterState.CoreUtilization < t.ApprovePolicy.CoreThreshold && clusterState.MemoryUtilization < t.ApprovePolicy.MemoryThreshold {
		availableMem := clusterState.TotalMemory - (clusterState.TotalMemory * clusterState.MemoryUtilization)
		availableCore := clusterState.TotalCore - (clusterState.TotalCore * clusterState.CoreUtilization)
		if availableCore >= uint(contract.Cores) && availableMem >= uint(contract.Memory) {
			// check incentive
			return true
		}
	}
	return false
}

type contractResHeap []*pb.ContractResponse

func (h contractResHeap) Len() int { return len(h) }

func (h contractResHeap) Less(i, j int) bool {
	return h[i].Price < h[j].Price
}

func (h contractResHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *contractResHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*pb.ContractResponse))
}

func (h *contractResHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (t *Trader) Trade() error {
	cs := t.State.getState()

	var contract *pb.ContractRequest
	if uint32(cs.CoreUtilization) > t.RequestPolicy.CoreMax || uint32(cs.MemoryUtilization) > t.RequestPolicy.MemoryMax {
		contract = calculateContractRequest(trader.SchedulerClient)
	}

	traders, err := registry.GetProviders(registry.Trader)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	ch := make(chan *pb.ContractResponse)
	h := &contractResHeap{}
	heap.Init(h)
	for _, trader := range traders {
		if _, ok := t.TraderClients[trader]; !ok {
			conn, err := grpc.Dial(fmt.Sprintf(trader, grpc.WithTransportCredentials(insecure.NewCredentials())))
			if err != nil {
				return err
			}
			t.TraderClients[trader] = pb.NewTraderClient(conn)
		}

		wg.Add(1)
		cont := pb.ContractRequest{Cores: contract.Cores, Memory: contract.Memory, Time: contract.Time, Price: contract.Price}
		go RequestResources(t.TraderClients[trader], &cont, &wg, ch)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for {
		select {
		case cont, ok := <-ch:
			if !ok {
				// all rountines finished
				break
			}
			// add to heap
			if cont.Approve {
				heap.Push(h, cont)
			}
		case <-time.After(3 * time.Second):
			// time elapsed
			// close channel and return?
			break
		default:
			time.Sleep(50 * time.Millisecond)
		}

	}

	// contracts in heap!

	// take first contract
}

func (t *Trader) RequestPolicyMonitor() {
	for {
		cs := t.State.getState()

		if uint32(cs.CoreUtilization) > t.RequestPolicy.CoreMax || uint32(cs.MemoryUtilization) > t.RequestPolicy.MemoryMax {
			t.Trade()
		}

		time.Sleep(10 * time.Second)
	}
}

var trader Trader

func Run(schedURL string, URL string, schedClient pb.ResourceChannelClient) {
	trader.SchedulerURL = schedURL
	trader.URL = URL
	trader.SchedulerClient = schedClient

	//trader.initialize_tracer()
	trader.newTrader()

	go getClusterState(trader.SchedulerClient)
	go trader.RequestPolicyMonitor()
}

func SetTracer(t trace.Tracer) {
	trader.Tracer = t
}
