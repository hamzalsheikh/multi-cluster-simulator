package trader

import (
	"container/heap"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/registry"
	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"

	api "go.opentelemetry.io/otel/metric"
)

type Trader struct {
	Name                string
	URL                 string
	State               clusterState
	ApprovePolicy       approvePolicy
	RequestPolicies     []requestPolicy
	SchedulerURL        string
	Tracer              trace.Tracer
	SchedulerClient     pb.ResourceChannelClient   // gRPC client
	TraderClients       map[string]pb.TraderClient // gRPC client
	meter               api.Meter
	Budget              uint32
	MaximimumCoreCost   uint32 // per second
	MaximimumMemoryCost uint32 // per second
}

func (t *Trader) newTrader() {
	// request cluster information & this can include more information
	// in the future
	// key exchange ?
	t.State.mutex = new(sync.Mutex)
}

func SetMeter(m api.Meter) {
	trader.meter = m
}

type clusterState struct {
	TotalMemory       uint
	TotalCore         uint
	MemoryUtilization uint
	CoreUtilization   uint
	AverageWaitTime   int
	mutex             *sync.Mutex
}

func (c *clusterState) setState(state clusterState) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.TotalMemory, c.TotalCore = state.TotalMemory, state.TotalCore
	c.CoreUtilization, c.MemoryUtilization = state.CoreUtilization, state.MemoryUtilization
	c.AverageWaitTime = state.AverageWaitTime

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
		AverageWaitTime:   c.AverageWaitTime,
	}
}

type approvePolicy struct {
	MemoryThreshold        uint
	CoreThreshold          uint
	MinimumCoreIncentive   uint // per second
	MinimumMemoryIncentive uint // per second
}

type requestPolicy interface {
	Broken(cs clusterState) bool
}

type requestPolicy_Utilization struct {
	MemoryMax uint32
	CoreMax   uint32
	Budget    uint32 // budget per request
}

func (r requestPolicy_Utilization) Broken(cs clusterState) bool {

	return uint32(cs.CoreUtilization) > r.CoreMax || uint32(cs.MemoryUtilization) > r.MemoryMax
}

type requestPolicy_WaitTime struct {
	MaximumWaittime int
	Budget          int
}

func (r requestPolicy_WaitTime) Broken(cs clusterState) bool {
	return cs.AverageWaitTime > r.MaximumWaittime
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

func (t *Trader) Trade(contract *pb.ContractRequest) error {

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
loop:
	for {
		select {
		case cont, ok := <-ch:
			if !ok {
				// all rountines finished
				break loop
			}
			// add to heap
			if cont.Approve {
				heap.Push(h, cont)
			}
		case <-time.After(3 * time.Second):
			// time elapsed
			// close channel and return?
			break loop
		default:
			time.Sleep(50 * time.Millisecond)
		}

	}

	// contracts in heap

	// take first contract
	for h.Len() > 0 {
		cont := heap.Pop(h).(*pb.ContractResponse)
		node, err := ApproveContract(trader.TraderClients[cont.Trader], cont)
		if err == nil {
			sendVirtualNode(trader.SchedulerClient, node)
			return nil
		}
	}
	return errors.New("couldn't borrow resources")
}

func (t *Trader) RequestPolicyMonitor() {
	for {
		fmt.Printf("in request policy monitor\n")
		cs := t.State.getState()
		for _, policy := range t.RequestPolicies {
			policyType := reflect.TypeOf(policy)
			var contract *pb.ContractRequest
			if policyType == reflect.TypeOf(requestPolicy_Utilization{}) && policy.Broken(cs) {
				contract = calculateContractRequest(trader.SchedulerClient, smallNode)
				trader.Trade(contract)
			} else if policy.Broken(cs) {
				contract = calculateContractRequest(trader.SchedulerClient, fastNode)
				trader.Trade(contract)
			}
		}
		time.Sleep(10 * time.Second)
	}
}

var trader Trader

func Run(schedURL string, URL string, schedClient pb.ResourceChannelClient) {
	fmt.Printf("in trader run!\n")
	trader.SchedulerURL = schedURL
	trader.URL = URL
	trader.SchedulerClient = schedClient

	//trader.initialize_tracer()
	trader.newTrader()

	getClusterState(trader.SchedulerClient)
	go trader.RequestPolicyMonitor()
}

func SetTracer(t trace.Tracer) {
	trader.Tracer = t
}
