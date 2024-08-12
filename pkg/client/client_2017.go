package client

import (
	"cmp"
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/scheduler"
)

type timeTable struct {
	start int
	jobs  []scheduler.Job
}

func (c *Client) sendBatchJobsAlibaba(traceFilePath string) {
	pwd, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(pwd)
	timeTables, err := parseBatchInstance(traceFilePath, 0.1)

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

func parseBatchInstance(filePath string, scale float32) ([]timeTable, error) {
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
				MemoryNeeded: uint(mem * 8000),
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
		fmt.Printf("last start: %v last job: %v\n", currentStart, currentJobs[len(currentJobs)-1])
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

type parseJob struct {
	Id           uint
	MemoryNeeded uint
	CoresNeeded  uint
	Start        int
	End          int
}

func parseContainerToJobs(filePath string, tt []timeTable, scale float32) ([]timeTable, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	jobMap := make(map[int]parseJob)

	for _, row := range rows {
		id, _ := strconv.Atoi(row[2])
		memoryNeeded, _ := strconv.ParseFloat(row[5], 64)
		cores := len(strings.Split(row[7], "|"))
		timeStamp, _ := strconv.Atoi(row[3])

		if row[1] == "Create" {
			if job, exists := jobMap[id]; exists {
				duration := int(float32(job.End-timeStamp) * scale)
				if duration < 0 {
					continue
				}
				j := scheduler.Job{
					Id:           uint(id),
					MemoryNeeded: uint(memoryNeeded * 8000),
					CoresNeeded:  uint(cores),
					Duration:     time.Duration(duration) * time.Second,
				}

				n, found := slices.BinarySearchFunc(tt, timeTable{start: job.Start}, func(a, b timeTable) int {
					return cmp.Compare(a.start, b.start)
				})
				if found {
					tt[n].jobs = append(tt[n].jobs, j)
				} else {
					tt = slices.Insert(tt, n, timeTable{
						start: timeStamp,
						jobs:  []scheduler.Job{j},
					})
				}
				delete(jobMap, id)
			} else {
				jobMap[id] = parseJob{
					Id:           uint(id),
					MemoryNeeded: uint(memoryNeeded * 8000),
					CoresNeeded:  uint(cores),
					Start:        timeStamp,
				}
			}
		} else if row[1] == "Delete" {
			if job, exists := jobMap[id]; exists {
				duration := int(float32(timeStamp-job.Start) * scale)
				if duration < 0 {
					continue
				}
				j := scheduler.Job{
					Id:           uint(id),
					MemoryNeeded: uint(memoryNeeded * 8000),
					CoresNeeded:  uint(cores),
					Duration:     time.Duration(duration) * time.Second,
				}

				n, found := slices.BinarySearchFunc(tt, timeTable{start: job.Start}, func(a, b timeTable) int {
					return cmp.Compare(a.start, b.start)
				})
				if found {
					tt[n].jobs = append(tt[n].jobs, j)
				} else {
					tt = slices.Insert(tt, n, timeTable{
						start: timeStamp,
						jobs:  []scheduler.Job{j},
					})
				}
				delete(jobMap, id)
			} else {
				jobMap[id] = parseJob{
					Id:           uint(id),
					MemoryNeeded: uint(memoryNeeded * 8000),
					CoresNeeded:  uint(cores),
					End:          timeStamp,
				}
			}
		}
	}
	return tt, nil
}
