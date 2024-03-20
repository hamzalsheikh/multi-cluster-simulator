package trader

import (
	"context"
	"fmt"
	"io"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
)

func getClusterState(client pb.ResourceChannelClient) {
	ctx := context.Background()
	stream, err := client.Start(ctx, nil)
	if err != nil {
		// TODO: error handling
		fmt.Println(err)
		return
	}

	for {
		state, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		// update trader values
		trader.State.mutex.Lock()
		trader.State.CoreUtilization, trader.State.MemoryUtilization = uint(state.CpuUtilization), uint(state.MemoryUtilization)
		fmt.Printf("mem: %v cpu: %v", trader.State.MemoryUtilization, trader.State.CoreUtilization)
		trader.State.mutex.Unlock()
	}

}
