package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"unicode"
)

// Map function produces a key/value pair for each input data.
func Map(data string) Pair {
	return Pair{
		key:   data,
		value: 1,
	}
}

// Reduce function takes a key and a list of values and produces a single output value.
func Reduce(key string, values []int) Pair {
	sum := 0
	for _, v := range values {
		sum += v
	}
	return Pair{
		key:   key,
		value: sum,
	}
}

func chunker(words []string, chunkSize int) [][]string {
	var chunks [][]string

	for i := 0; i < len(words); i += chunkSize {
		end := min(i+chunkSize, len(words))
		chunks = append(chunks, words[i:end])
	}
	return chunks
}

func main() {
	var chunks [][]string

	var partitionWg sync.WaitGroup
	var workerWg sync.WaitGroup

	pairChan := make(chan Pair)

	master := Master{
		MapTasks: make(map[int]*MapTask),
		Workers:  make(map[int]*Worker),
	}

	workerCount := 4
	reducerCount := 3

	workerWg.Add(workerCount)

	entries, err := os.ReadDir("data")
	if err != nil {
		fmt.Println("Error reading directory.")
		return
	}
	os.RemoveAll("intermediate")
	os.Mkdir("intermediate", 0o755)

	// Shuffle, grouping, and reduce

	partitionWg.Go(func() {
		for pair := range pairChan {
			partition := int(hash(pair.key)) % reducerCount
			filename := fmt.Sprintf("intermediate/partition_%d", partition)
			f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				fmt.Println("Error opening file.")
				return
			}
			if _, err := fmt.Fprintf(f, "%s,%d\n", pair.key, pair.value); err != nil {
				fmt.Println("Error writing to file.")
				return
			}
			f.Close()
		}
	})

	taskID := 0

	for _, entry := range entries {
		path := "data/" + entry.Name()
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Println("Error reading file.")
			return
		}

		content := string(data)
		cleaned := strings.Map(func(r rune) rune {
			if unicode.IsPunct(r) {
				return -1
			}
			return r
		}, content)

		words := strings.Fields(cleaned)
		chunks = chunker(words, 2)

		// Mapping

		for _, chunk := range chunks {
			task := &MapTask{
				ID:    taskID,
				State: Pending,

				Words: chunk,
			}
			master.MapTasks[taskID] = task
			taskID++
		}
	}
	for i := range workerCount {
		master.mu.Lock()
		master.Workers[i] = &Worker{
			ID:    i,
			State: Idle,
		}
		master.mu.Unlock()
		go master.worker(Worker{ID: i}, pairChan, &workerWg)
	}

	master.ReduceTasks = make(map[int]*ReduceTask)
	for i := range reducerCount {
		master.ReduceTasks[i] = &ReduceTask{
			ID:        i,
			State:     Pending,
			Partition: i,
		}
	}

	workerWg.Wait()
	// after all the mappers are done, we can close the pairChan to signal the partitioner that there are no more pairs to process
	close(pairChan)

	// Wait for partitioning and reducing to finish
	partitionWg.Wait()
}
