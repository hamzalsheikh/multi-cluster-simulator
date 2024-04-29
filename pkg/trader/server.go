package trader

import (
	"context"
	"sync"
	"time"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type traderServer struct {
	pb.UnimplementedTraderServer

	currentContract pb.ContractResponse
	currLock        *sync.Mutex
	id              uint32
}

func NewTraderServer() *traderServer {
	// initialize server
	s := &traderServer{}
	return s
}

func (s *traderServer) RequestResource(ctx context.Context, contract *pb.ContractRequest) (*pb.ContractResponse, error) {

	// trade in progress, refuse to start new trader
	s.currLock.Lock()
	if s.currentContract.Id == 0 {
		s.currLock.Unlock()
		return &pb.ContractResponse{Approve: false}, nil
	}
	s.currLock.Unlock()
	// consult policy
	approve := trader.ApproveTrade(contract)

	contractResponse := pb.ContractResponse{Id: s.id, Approve: approve}
	s.id++

	go func() {
		time.Sleep(time.Second * 10)
		// check if current response is still there, delete initialize
		s.currLock.Lock()
		defer s.currLock.Unlock()
		if s.currentContract.Id != 0 {
			s.currentContract = pb.ContractResponse{}
		}
	}()

	// this will eventually be a stream for negotiation
	return &contractResponse, nil
}

func (s *traderServer) ApproveContract(ctx context.Context, contract *pb.ContractResponse) (*pb.NodeObject, error) {
	s.currLock.Lock()
	defer s.currLock.Unlock()

	c := s.currentContract
	if c.Id != contract.Id {
		return &pb.NodeObject{}, status.Errorf(codes.DeadlineExceeded, "Time limit exceeded, contract denied")
	}

	// contract valid, request resources from scheduler
	virtualNode, err := getVirtualNode(trader.SchedulerClient, &pb.VirtualNodeRequest{Cores: contract.Cores, Memory: contract.Memory, Time: contract.Time})

	// reinitialize currentContract for future activity
	s.currentContract = pb.ContractResponse{}
	return virtualNode, err
}
