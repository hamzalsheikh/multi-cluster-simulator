package trader

import (
	"container/heap"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/registry"
	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
	"github.com/rs/zerolog"

	api "go.opentelemetry.io/otel/metric"
)

type Trader struct {
	Name                string
	URL                 string
	State               clusterState
	ApprovePolicy       approvePolicy
	RequestPolicies     []requestPolicy
	SchedulerURL        string
	SchedulerClient     pb.ResourceChannelClient   // gRPC client
	TraderClients       map[string]pb.TraderClient // gRPC client
	Budget              float32
	BudgetMutex         *sync.Mutex
	MaximimumCoreCost   float32 // per second
	MaximimumMemoryCost float32 // per second
	Logger              zerolog.Logger
	meter               api.Meter
	Tracer              trace.Tracer
}

func (t *Trader) newTrader(policy string) {
	// request cluster information & this can include more information
	// in the future
	// key exchange ?
	t.Logger.Info().Msg("Initializing Trader")
	t.Logger.Info().Msgf("trader URL: %s", t.URL)
	t.ApprovePolicy = approvePolicy{
		MemoryThreshold:        0.8,
		CoreThreshold:          0.8,
		MinimumCoreIncentive:   -1,
		MinimumMemoryIncentive: -1,
	}
	t.Budget = 1000
	t.MaximimumCoreCost = 0.000003
	t.MaximimumMemoryCost = 0.000003

	switch policy {
	case "waitTime":
		t.Logger.Info().Msg("request policy is waitTime")
		t.RequestPolicies = append(t.RequestPolicies,
			requestPolicy_WaitTime{
				MaximumWaittime: 600000,
			})
	case "utilization":
		t.Logger.Info().Msg("request policy is utilization")
		t.RequestPolicies = append(t.RequestPolicies,
			requestPolicy_Utilization{
				MemoryMax: 0.8,
				CoreMax:   0.8,
			})
	case "both":
		t.Logger.Info().Msg("request policy is both")

		t.RequestPolicies = append(t.RequestPolicies,
			requestPolicy_WaitTime{
				MaximumWaittime: 600000,
			},
			requestPolicy_Utilization{
				MemoryMax: 0.8,
				CoreMax:   0.8,
			})
	}
	t.State.mutex = new(sync.Mutex)
	t.BudgetMutex = new(sync.Mutex)
	t.TraderClients = make(map[string]pb.TraderClient)
}

func SetMeter(m api.Meter) {
	trader.meter = m
}

type clusterState struct {
	TotalMemory       uint
	TotalCore         uint
	MemoryUtilization float32
	CoreUtilization   float32
	AverageWaitTime   float64
	mutex             *sync.Mutex
}

func (c *clusterState) setState(state clusterState) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.TotalMemory, c.TotalCore = state.TotalMemory, state.TotalCore
	c.CoreUtilization, c.MemoryUtilization = state.CoreUtilization, state.MemoryUtilization
	c.AverageWaitTime = state.AverageWaitTime

}

func (c *clusterState) setUtilization(state *pb.ClusterState) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.CoreUtilization, c.MemoryUtilization, c.AverageWaitTime = state.CoresUtilization, state.MemoryUtilization, state.AverageWaitTime
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
	MemoryThreshold        float32
	CoreThreshold          float32
	MinimumCoreIncentive   float64 // per second
	MinimumMemoryIncentive float64 // per second
}

type requestPolicy interface {
	Broken(cs clusterState) bool
}

type requestPolicy_Utilization struct {
	MemoryMax float32
	CoreMax   float32
	Budget    uint32 // budget per request
}

func (r requestPolicy_Utilization) Broken(cs clusterState) bool {

	return cs.CoreUtilization > r.CoreMax || cs.MemoryUtilization > r.MemoryMax
}

type requestPolicy_WaitTime struct {
	MaximumWaittime float64 // in Milliseconds
	Budget          int
}

func (r requestPolicy_WaitTime) Broken(cs clusterState) bool {
	return cs.AverageWaitTime > r.MaximumWaittime
}

// incentive modeled over Azure Cloud Services
func (t *Trader) GetMinimumIncentive(coreUtil float32, memUtil float32) float64 {
	// if budget is -1, environment is cooperative
	budget := t.GetBudget()
	if budget == -1 {
		return 0
	}
	if coreUtil < 0.4 && memUtil < 0.4 {
		return 0.00000075
	} else if coreUtil < 0.6 && memUtil < 0.6 {
		return 0.0000015
	} else {
		return 0.000003
	}
}

func (t *Trader) ApproveTrade(ctx context.Context, contract *pb.ContractRequest) bool {
	t.Logger.Info().Msg("in ApproveTrade()")
	_, span := t.Tracer.Start(ctx, "ApproveTrade")
	defer span.End()

	clusterState := t.State.getState()
	if clusterState.CoreUtilization < t.ApprovePolicy.CoreThreshold && clusterState.MemoryUtilization < t.ApprovePolicy.MemoryThreshold {
		availableMem := float32(clusterState.TotalMemory) - (float32(clusterState.TotalMemory) * clusterState.MemoryUtilization)
		availableCore := float32(clusterState.TotalCore) - (float32(clusterState.TotalCore) * clusterState.CoreUtilization)
		t.Logger.Info().Msgf("available mem: %v and core: %v --> contract mem %v core %v", availableMem, availableCore, contract.Memory, contract.Cores)
		if availableCore >= float32(contract.Cores) && availableMem >= float32(contract.Memory) {
			// check incentive
			// If traders are not trading with incentives, price is expected to be 0 and the minimum price would be negative
			//incentive := t.ApprovePolicy.MinimumCoreIncentive*float64(contract.Cores)*contract.Time.AsDuration().Seconds() + t.ApprovePolicy.MinimumMemoryIncentive*float64(contract.Memory)*contract.Time.AsDuration().Seconds()
			incentive := t.GetMinimumIncentive(clusterState.CoreUtilization, clusterState.MemoryUtilization) * contract.Time.AsDuration().Seconds()
			if float64(contract.Price) >= incentive {
				t.Logger.Info().Msgf("Approved trade with contract %+v", contract)
				span.AddEvent("Approved trade")
				return true
			}
			t.Logger.Info().Msgf("Price %v is less than minimum incentive %v", contract.Price, incentive)
		}
	}

	span.AddEvent("Trade not approved")
	t.Logger.Info().Msgf("Couldn't approve trade with contract ID: %v", contract.Id)
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

func (t *Trader) Trade(ctx context.Context, contract *pb.ContractRequest) error {
	t.Logger.Info().Msg("In Trade()")
	ctx, span := t.Tracer.Start(ctx, "trade initiated")
	defer span.End()

	traders, err := registry.GetProviders(registry.Trader)
	span.AddEvent("got a list of traders from registry")
	t.Logger.Info().Msg("got a list of traders from registry")
	if err != nil {
		t.Logger.Error().Err(err)
		return err
	}

	var wg sync.WaitGroup
	ch := make(chan *pb.ContractResponse)
	h := &contractResHeap{}
	heap.Init(h)

	for _, trader := range traders {
		if trader == t.URL {
			continue
		}
		if _, ok := t.TraderClients[trader]; !ok {
			conn, err := grpc.NewClient(fmt.Sprint(trader), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
			if err != nil {
				t.Logger.Error().Err(err)
				continue
			}
			t.TraderClients[trader] = pb.NewTraderClient(conn)
		}

		span.AddEvent("connection to trader successfully created")
		t.Logger.Info().Msgf("connection to trader %s successfully created", trader)
		wg.Add(1)
		cont := pb.ContractRequest{Id: contract.Id, Cores: contract.Cores, Memory: contract.Memory, Time: contract.Time, Price: contract.Price, Trader: trader}
		go RequestResources(ctx, t.TraderClients[trader], &cont, &wg, ch)
	}
	// non-blocking go routine to close channel for
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
				t.Logger.Info().Msg("All traders replied")
				break loop
			}
			// add to heap
			if cont.Approve {
				heap.Push(h, cont)
			}
		case <-time.After(3 * time.Second):
			// time elapsed
			// close channel and return?
			t.Logger.Info().Msg("time limit reached")
			break loop
		default:
			time.Sleep(50 * time.Millisecond)
		}

	}

	// contracts in heap

	span.AddEvent("contracts received")
	t.Logger.Info().Msg("contracts received")
	// take first contract
	for h.Len() > 0 {
		cont := heap.Pop(h).(*pb.ContractResponse)
		t.Logger.Info().Msgf("winner trader url:  %s", cont.Trader)
		node, err := ApproveContract(ctx, trader.TraderClients[cont.Trader], cont)
		if err == nil {
			t.Logger.Info().Msgf("received virtual node from trader memory %v", node.Memory)
			sendVirtualNode(ctx, trader.SchedulerClient, node)
			return nil
		} else {
			t.Logger.Error().Err(err).Msg("couldn't acquire resources")
		}
	}
	return errors.New("couldn't acquire resources")
}

func (t *Trader) RequestPolicyMonitor() {

	t.Logger.Info().Msg("Request policy monitor Initiated")
	for {
		cs := t.State.getState()
		for _, policy := range t.RequestPolicies {
			policyType := reflect.TypeOf(policy)
			var contract *pb.ContractRequest
			if policyType == reflect.TypeOf(requestPolicy_Utilization{}) && policy.Broken(cs) {
				ctx, span := t.Tracer.Start(context.Background(), "utilization policy broken")
				t.Logger.Info().Msg("utilization policy broken")
				contract = calculateContractRequest(ctx, trader.SchedulerClient, smallNode)
				if contract.Time == nil || contract.Cores == 0 || contract.Memory == 0 {
					t.Logger.Info().Msg("invalid contract")
					continue
				}
				t.Logger.Info().Msgf("contract created %v", contract)
				budget := t.GetBudget()
				if budget != -1 {
					price := t.MaximimumCoreCost*float32(contract.Time.Seconds) + t.MaximimumMemoryCost*float32(contract.Time.Seconds)
					if price > budget {
						contract.Price = budget
					} else {
						contract.Price = price
					}
				}
				t.Logger.Info().Msgf("contract created %v", contract.Id)
				err := trader.Trade(ctx, contract)
				span.End()
				// wait after trade, arbitrary numbers used here,
				if err == nil {
					t.Logger.Info().Msg("Trade was successful")

					if t.GetBudget() != -1 {
						t.SetBudget(-contract.Price)
					}
					time.Sleep(2 * time.Minute)
				} else {
					t.Logger.Info().Msg("Trade was not successul, waiting before initiating a new trade")
					time.Sleep(1 * time.Minute)
				}

			} else if policy.Broken(cs) {
				ctx, span := t.Tracer.Start(context.Background(), "wait time policy broken")

				t.Logger.Info().Msg("wait time policy broken")
				contract = calculateContractRequest(ctx, trader.SchedulerClient, fastNode)
				if contract.Time == nil || contract.Cores == 0 || contract.Memory == 0 {
					t.Logger.Info().Msg("invalid contract")
					continue
				}
				budget := t.GetBudget()
				if budget != -1 {
					price := t.MaximimumCoreCost*float32(contract.Time.Seconds) + t.MaximimumMemoryCost*float32(contract.Time.Seconds)
					if price > budget {
						contract.Price = budget
					} else {
						contract.Price = price
					}
				}
				t.Logger.Info().Msgf("contract created %v", contract.Id)
				err := trader.Trade(ctx, contract)

				span.End()
				// wait after trade, arbitrary numbers used here,
				if err == nil {
					t.Logger.Info().Msg("Trade was successful")
					if t.GetBudget() != -1 {
						t.SetBudget(-contract.Price)
					}
					time.Sleep(2 * time.Minute)
				} else {
					t.Logger.Info().Msg("Trade was not successul, waiting before initiating a new trade")
					time.Sleep(1 * time.Minute)
				}
			}
		}
		time.Sleep(10 * time.Second)
		t.Logger.Info().Msgf("budget is %v", t.GetBudget())
	}
}

var trader Trader

func Run(schedURL string, URL string, schedClient pb.ResourceChannelClient, logger zerolog.Logger, policy string) {
	trader.Logger.Info().Msg("In trader Run()")
	trader.SchedulerURL = schedURL
	trader.URL = URL
	trader.Logger.Info().Msgf("trader URL: %s", trader.URL)
	trader.SchedulerClient = schedClient
	trader.Logger = logger
	//trader.initialize_tracer()
	trader.newTrader(policy)

	go trader.RequestPolicyMonitor()
	go trader.ReportBudget()
	getClusterState(trader.SchedulerClient)
}

func SetTracer(t trace.Tracer) {
	trader.Tracer = t
}

func (t *Trader) GetBudget() float32 {
	t.BudgetMutex.Lock()
	defer t.BudgetMutex.Unlock()

	return t.Budget
}

func (t *Trader) SetBudget(diff float32) {
	t.BudgetMutex.Lock()
	defer t.BudgetMutex.Unlock()

	t.Budget += diff
}

func (t *Trader) ReportBudget() {
	// Create a new CSV file

	err := os.Mkdir("data", 0750)
	if err != nil && !os.IsExist(err) {
		t.Logger.Fatal().Err(err).Msgf("error creating data directory")
	}

	file, err := os.Create("data/" + os.Getenv("SERVICE_NAME") + "_budget.csv")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	// Create a new CSV writer
	writer := csv.NewWriter(file)

	err = writer.Write(
		[]string{
			"time",
			"budget",
		},
	)
	if err != nil {
		t.Logger.Error().Err(err).Msgf("Couldn't input header")
	}

	var time_stamp int64
	for {

		err := writer.Write(
			[]string{
				strconv.FormatInt(time_stamp, 10),
				strconv.FormatFloat(float64(t.GetBudget()), 'f', 2, 64),
			},
		)
		if err != nil {
			t.Logger.Error().Err(err).Msgf("Couldn't record budget")
		}

		time_stamp += 1
		writer.Flush()
		time.Sleep(3 * time.Second)
	}
}
