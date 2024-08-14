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
	"time"

	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/scheduler"
)

func parseBatchInstance_2018(filePath string, scale float32, offset int) ([]timeTable, error) {
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
		start, _ := strconv.Atoi(record[5])
		end, _ := strconv.Atoi(record[6])
		// job should not be in trace
		if start < 0 && end < 0 {
			continue
		}

		if start > end {
			continue
		}
		if start < 0 {
			start = 0
		}

		duration := float32(end-start) * scale

		if duration < 0 {
			fmt.Printf("Error, duration cannot be negative, start %v, end %v", start, end)
			return nil, fmt.Errorf("Error, duration cannot be negative")
		}

		mem, err := strconv.ParseFloat(record[13], 32)
		if err != nil {
			fmt.Print("couldn't parse memory")
			continue
		}
		core, err := strconv.ParseFloat(record[11], 32)
		if err != nil {
			fmt.Print("couldn't parse core")
			continue
		}

		job := scheduler.Job{
			Id:           uint(rand.Int()),
			CoresNeeded:  uint(math.Ceil(core / 100)),
			MemoryNeeded: uint(mem * 1200),
			Duration:     time.Duration(float32(duration)*scale) * time.Second,
		}
		rows = append(rows, RowData{start: int(float32(start-offset) * scale), job: job})

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

func parseContainerToJobs_2018(filePath string, tt []timeTable, scale float32, offset int, maxTime int) ([]timeTable, error) {
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

	for _, row := range rows {
		id := rand.Int()
		memoryNeeded, _ := strconv.ParseFloat(row[7], 64)
		cores, _ := strconv.ParseFloat(row[5], 64)
		timeStamp, _ := strconv.Atoi(row[2])

		if row[1] != "started" || timeStamp > maxTime {
			continue
		}
		var start int
		if timeStamp > offset {
			start = timeStamp
		} else {
			start = offset
		}

		duration := float32(maxTime-start) * scale
		if duration < 0 {
			continue
		}
		j := scheduler.Job{
			Id:           uint(id),
			MemoryNeeded: uint(memoryNeeded * 1200),
			CoresNeeded:  uint(math.Ceil(cores / 100)),
			Duration:     time.Duration(float32(duration)*scale) * time.Second,
		}

		n, found := slices.BinarySearchFunc(tt, timeTable{start: start - offset}, func(a, b timeTable) int {
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
	}
	return tt, nil
}
