package client

import "sandbox/scheduler"

type Client struct {
	Name         string
	Cluster      scheduler.Cluster
	SchedulerURL string
}

func SetSchedURL(URL string) {
	client.SchedulerURL = URL
}
