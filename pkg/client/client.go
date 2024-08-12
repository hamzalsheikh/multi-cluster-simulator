package client

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"time"

	exprand "golang.org/x/exp/rand"

	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/scheduler"

	"go.opentelemetry.io/otel/trace"
	"gonum.org/v1/gonum/stat/distuv"

	"encoding/csv"
	"os"
	"sort"
	"strconv"
)

type Client struct {
	Name         string
	URL          string
	Cluster      scheduler.Cluster
	SchedulerURL string
	maxJobMem    uint
	maxJobCore   uint
	time_dist    string
	job_dist     string
	Tracer       trace.Tracer
}

func Run(schedURL string, URL string, trace bool, traceFilePath string) {
	client.SchedulerURL = schedURL
	client.URL = URL
	// production workload
	if trace {
		client.sendJobsAlibaba(traceFilePath)
		return
	}
	// synthatic workload
	client.newClient()
	client.sendJobs()
}

func SetTracer(t trace.Tracer) {
	client.Tracer = t
}

func (c *Client) newClient() {
	// request cluster information & this can include more information
	// in the future
	// key exchange ?
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
	// TODO: change this to user input
	c.time_dist = "poisson"

	// set job range
	c.setMaxCluster()
}

func (c *Client) setMaxCluster() {
	var maxCores uint
	var maxMem uint

	for _, node := range c.Cluster.Nodes {
		if node.Memory > maxMem {
			maxMem = node.Memory
		}
		if node.Cores > maxCores {
			maxCores = node.Cores
		}
	}

	c.maxJobCore = maxCores
	c.maxJobMem = maxMem
}

func (c *Client) sendJobs() {
	// approximating normal distribution
	job_dist := distuv.Beta{
		Alpha: 2,
		Beta:  2,
	}
	var id uint
	getJob := func() scheduler.Job {

		var j scheduler.Job
		j.Id = id
		// fmt.Print(c.maxJobCore)
		j.CoresNeeded = uint(job_dist.Rand() * float64(c.maxJobCore)) // uint(rand.Intn(int(c.maxJobCore))) //uint( dist.Rand() * float64(c.maxJobCore))
		j.Duration = 1 + time.Duration(rand.Intn(30))*time.Second
		j.MemoryNeeded = uint(job_dist.Rand() * float64(c.maxJobMem)) // uint(rand.Intn(int(c.maxJobMem)))
		id++
		return j
	}

	// create time distrribution
	switch c.time_dist {
	case "poisson":
		time_dist := distuv.Poisson{
			Lambda: 100,
			Src:    exprand.NewSource(9),
		}

		for {
			// each for loop is one minute, lambda jobs per minute is sent
			jobs := int(time_dist.Rand())
			// fmt.Printf("jobs per minute: %v", jobs)
			time_between_jobs := 60000 / jobs
			i := 0
			for i < jobs {
				i++
				// uint(dist.Rand() * float64(c.maxJobMem))
				//fmt.Printf("cores: %v memory %v\n", dist.Rand(), dist.Rand())
				j := getJob()
				SendJob(j)
				fmt.Printf("Job %+v sent\n", j)
				time.Sleep(time.Duration(time_between_jobs) * time.Millisecond)

			}

		}

	case "weibull":
		time_dist := distuv.Weibull{
			Lambda: 10,
			K:      3,
		}

		for {

			j := getJob()
			SendJob(j)
			// weibull time between jobs
			// 63% is less than lambda seconds
			time.Sleep(time.Duration(time_dist.Rand()) * time.Second)

		}
	}
}

type timeTable struct {
	start int
	jobs  []scheduler.Job
}

func (c *Client) sendJobsAlibaba(traceFilePath string) {
	pwd, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(pwd)
	timeTables, err := parseCSVAndPopulateTimeTable(traceFilePath, 0.1)

	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	prev := 0
	for _, t := range timeTables {
		time.Sleep(time.Duration(t.start) - time.Duration(prev))
		for _, j := range t.jobs {
			SendJob(j)
			fmt.Printf("Job %+v sent\n", j)
		}
		prev = t.start
	}
	fmt.Println("Finished sending all trace jobs")
}

func parseCSVAndPopulateTimeTable(filePath string, scale float32) ([]timeTable, error) {
	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read the CSV file
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	fmt.Printf("read all csv file\n")
	// Convert the records to a more usable format and process them
	type RowData struct {
		start int
		job   scheduler.Job
	}
	var rows []RowData

	for _, record := range records {
		start, _ := strconv.Atoi(record[0])
		end, _ := strconv.Atoi(record[1])
		// job should not be in trace
		if start < 0 && end < 0 {
			continue
		}

		if start < 0 {
			start = 0
		}

		if start > end {
			start, end = end, start
		}

		duration := int(float32(end-start) * scale)

		if duration < 0 {
			fmt.Printf("Error, duration cannot be negative, start %v, end %v", start, end)
			return nil, fmt.Errorf("Error, duration cannot be negative")
		}

		if record[5] == "Terminated" {
			mem, err := strconv.ParseFloat(record[9], 32)
			if err != nil {
				fmt.Print("couldn't parse memory")
				continue
			}

			core, err := strconv.ParseFloat(record[7], 32)
			if err != nil {
				fmt.Print("couldn't parse core")
				continue
			}

			job := scheduler.Job{
				Id:           uint(rand.Int()),
				CoresNeeded:  uint(math.Ceil(core)),
				MemoryNeeded: uint(mem * float64(8000)),
				Duration:     time.Duration(duration) * time.Second,
			}
			rows = append(rows, RowData{start: int(float32(start) * scale), job: job})
		}
	}
	fmt.Printf("filtered only the terminated\n")
	// Sort the rows by the start time
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].start < rows[j].start
	})
	fmt.Printf("sorted rows %v\n", len(rows))

	// Create the timeTable structs
	var timeTables []timeTable
	currentStart := -1
	var currentJobs []scheduler.Job

	for _, row := range rows {
		if row.start != currentStart {
			if currentStart != -1 {
				timeTables = append(timeTables, timeTable{start: currentStart, jobs: currentJobs})
			}
			currentStart = row.start
			currentJobs = []scheduler.Job{row.job}
		} else {
			currentJobs = append(currentJobs, row.job)
		}
	}

	// Append the last timeTable
	if currentStart != -1 {
		timeTables = append(timeTables, timeTable{start: currentStart, jobs: currentJobs})
	}
	fmt.Printf("created timetable\n")
	return timeTables, nil
}

func atoiOrDefault(s string, def int) int {
	value, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return value
}
