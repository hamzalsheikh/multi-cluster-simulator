package registry

type Registration struct {
	ServiceName      ServiceName
	ServiceURL       string
	RequiredServices []ServiceName
	ServiceUpdateURL string // how the registry talk back to the service
	HeartbeatURL     string
}

type ServiceName string

const (
	LogService     = ServiceName("LogServiced")
	GradingService = ServiceName("GradingService")
	TeacherPortal  = ServiceName("TeacherPortal")
	Scheduler      = ServiceName("Scheduler")
)

type patchEntry struct {
	Name ServiceName
	URL  string
}

type patch struct {
	Added   []patchEntry
	Removed []patchEntry
}
