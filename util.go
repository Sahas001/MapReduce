package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"sync"
)

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func mapperWorker(
	w Worker,
	tasks <-chan MapTask,
	pairChan chan<- Pair,
	doneChan chan<- TaskResult,
	wg *sync.WaitGroup,
) {
	for task := range tasks {
		for _, word := range task.Words {
			pairChan <- Map(word)
		}

		doneChan <- TaskResult{
			TaskID:   task.ID,
			WorkerID: w.ID,
		}

		wg.Done()
	}
}

func reducerWorker(id int, wg *sync.WaitGroup) {
	defer wg.Done()

	grouped := make(map[string][]int)

	path := fmt.Sprintf("intermediate/partition_%d", id)
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error opening file.")
		return
	}

	scanner := bufio.NewScanner(file)
	defer file.Close()

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ",")

		value, _ := strconv.Atoi(parts[1])

		pair := Pair{
			key:   parts[0],
			value: value,
		}

		grouped[pair.key] = append(grouped[pair.key], pair.value)

	}

	for key, values := range grouped {
		result := Reduce(key, values)

		fmt.Printf("(%s, %d)\n", result.key, result.value)
	}
}
