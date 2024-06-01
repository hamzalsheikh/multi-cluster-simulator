package trader

import (
	"context"
	"math/rand"
	"sync"
	"time"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type traderServer struct {
	pb.UnimplementedTraderServer

	currentContract *pb.ContractResponse
	currLock        *sync.Mutex
	id              uint32
}

func NewTraderServer() *traderServer {
	// initialize server
	s := &traderServer{}
	s.currLock = new(sync.Mutex)
	s.id = rand.Uint32()
	s.currentContract = &pb.ContractResponse{}
	return s
}

func (s *traderServer) RequestResource(ctx context.Context, contract *pb.ContractRequest) (*pb.ContractResponse, error) {
	trader.Logger.Info().Msg("In RequestResource()")
	// trade in progress, refuse to start new trader
	s.currLock.Lock()
	if s.currentContract.Id != 0 {
		s.currLock.Unlock()
		trader.Logger.Info().Msg("trader in progress, can't initiate new trade")
		return &pb.ContractResponse{Approve: false}, nil
	}
	s.currLock.Unlock()
	// consult policy
	approve := trader.ApproveTrade(ctx, contract)

	contractResponse := pb.ContractResponse{Id: s.id, Approve: approve, Trader: trader.URL, Cores: contract.Cores, Memory: contract.Memory, Time: contract.Time, Price: contract.Price}
	s.currentContract = &contractResponse
	s.id++

	go func() {
		time.Sleep(time.Second * 20)
		// check if current response is still there, delete initialize
		trader.Logger.Info().Msg("time limit for RequestResource() reached, deleting currentContract")
		s.currLock.Lock()
		defer s.currLock.Unlock()
		if s.currentContract.Id == contractResponse.Id {
			s.currentContract = &pb.ContractResponse{}
		}
	}()

	// this will eventually be a stream for negotiation
	return &contractResponse, nil
}

func (s *traderServer) ApproveContract(ctx context.Context, contract *pb.ContractResponse) (*pb.NodeObject, error) {
	trader.Logger.Info().Msgf("In ApproveContract() id %v", contract.Id)
	trader.Logger.Info().Msgf("contract id %v mem %v ", contract.Id, contract.Memory)
	s.currLock.Lock()
	defer s.currLock.Unlock()

	if s.currentContract.Id != contract.Id {
		return &pb.NodeObject{}, status.Errorf(codes.DeadlineExceeded, "Time limit exceeded, contract denied")
	}

	// contract valid, request resources from scheduler
	virtualNodeReq := &pb.VirtualNodeRequest{Cores: contract.Cores, Memory: contract.Memory, Time: contract.Time}

	trader.Logger.Info().Msgf("getting node from scheduler of request memory: %v", virtualNodeReq.Memory)
	virtualNode, err := getVirtualNode(ctx, trader.SchedulerClient, virtualNodeReq)

	if err != nil {
		trader.Logger.Error().Err(err).Msg("couldn't get virtual node from scheduler")
	}
	// reinitialize currentContract for future activity
	s.currentContract = &pb.ContractResponse{}
	return virtualNode, err
}
