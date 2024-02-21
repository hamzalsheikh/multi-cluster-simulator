package trader

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/scheduler"

	"go.opentelemetry.io/otel/trace"
)

type Trader struct {
	Name         string
	URL          string
	Cluster      scheduler.Cluster
	SchedulerURL string
	Tracer       trace.Tracer
}

var trader Trader

func Run(schedURL string, URL string) {
	trader.SchedulerURL = schedURL
	trader.URL = URL

	//trader.initialize_tracer()
	trader.newTrader()
}

func SetTracer(t trace.Tracer) {
	trader.Tracer = t
}

func (t *Trader) newTrader() {
	// request cluster information & this can include more information
	// in the future
	// key exchange ?
	res, err := http.Get(t.SchedulerURL + "/newTrader")
	if err != nil {
		log.Println(err)
		return
	}
	var cluster scheduler.Cluster
	dec := json.NewDecoder(res.Body)
	err = dec.Decode(&cluster)
	if err != nil {
		log.Println(err)
		return
	}
}

// policy logic goes here
