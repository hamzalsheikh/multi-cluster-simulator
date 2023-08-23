package registry

type Registration struct {
	ServiceName ServiceName
	ServiceURL  string
}

type ServiceName string

const (
	LogService     = ServiceName("LogServiced")
	GradingService = ServiceName("GradingService")
)
