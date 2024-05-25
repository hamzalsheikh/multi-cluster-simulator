package trader

import (
	"context"
	"sync"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
)

func RequestResources(ctx context.Context, client pb.TraderClient, contractReq *pb.ContractRequest, wg *sync.WaitGroup, ch chan<- *pb.ContractResponse) {
	defer wg.Done()

	contractRes, err := client.RequestResource(ctx, contractReq)
	if err != nil {
		return
	}
	ch <- contractRes
}

func ApproveContract(ctx context.Context, client pb.TraderClient, contractRsp *pb.ContractResponse) (*pb.NodeObject, error) {
	return client.ApproveContract(ctx, contractRsp)
}
