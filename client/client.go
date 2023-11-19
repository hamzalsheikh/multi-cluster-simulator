package client

import (
	"encoding/json"
	"log"
	"net/http"
	"sandbox/scheduler"
)

type Client struct {
	Name         string
	Cluster      scheduler.Cluster
	SchedulerURL string
	maxJobMem    int
	maxJobCore   int
}

func SetSchedURL(URL string) {
	client.SchedulerURL = URL
	client.newClient()
}

func (c *Client) newClient() {
	// request cluster information & this can include more information
	// in the future
	res, err := http.Get(c.SchedulerURL + "/newClient")
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
	c.Cluster = cluster
}
