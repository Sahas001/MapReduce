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

func (m *Master) mapperWorker(
	w Worker,
	pairChan chan<- Pair,
	wg *sync.WaitGroup,
) {
	for {
		task := m.RequestMapTask(w.ID)
		if task == nil {
			return
		}
		for _, word := range task.Words {
			pairChan <- Map(word)
		}
		m.MapTaskCompleted(task.ID, w.ID)
		wg.Done()
	}
}

func (m *Master) reducerWorker(id int, wg *sync.WaitGroup) {
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

func (m *Master) RequestMapTask(workerID int) *MapTask {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, task := range m.MapTasks {
		if task.State == Pending {
			task.State = Running
			m.Workers[workerID].State = Busy
			return task
		}
	}

	return nil
}

func (m *Master) MapTaskCompleted(taskID, workerID int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.MapTasks[taskID].State = Completed
	m.Workers[workerID].State = Idle
}
