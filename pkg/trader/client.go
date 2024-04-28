package trader

import (
	"context"
	"sync"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
)

func RequestResources(client pb.TraderClient, contractReq *pb.ContractRequest, wg *sync.WaitGroup, ch chan<- *pb.ContractResponse) {
	ctx := context.Background()
	defer wg.Done()

	contractRes, err := client.RequestResource(ctx, contractReq)
	if err != nil {
		return
	}
	ch <- contractRes
}
