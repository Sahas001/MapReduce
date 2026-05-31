package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (m *Master) worker(
	w Worker,
	pairChan chan<- Pair,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for {

		task := m.RequestTask(w.ID)

		switch task.Type {

		case MapTaskType:

			mapTask := m.MapTasks[task.TaskID]

			for _, word := range mapTask.Words {
				pairChan <- Map(word)
			}

			m.MapTaskCompleted(task.TaskID, w.ID)

		case ReduceTaskType:

			reduceTask := m.ReduceTasks[task.TaskID]
			path := fmt.Sprintf("intermediate/partition_%d", reduceTask.Partition)
			file, err := os.Open(path)
			if err != nil {
				fmt.Println("Error opening file.")
				m.ReduceTaskCompleted(task.TaskID, w.ID)
				continue
			}

			grouped := make(map[string][]int)

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
			m.ReduceTaskCompleted(task.TaskID, w.ID)

		case WaitTaskType:
			time.Sleep(100 * time.Millisecond)
			continue

		case ExitTaskType:
			return
		}
	}
}

func (m *Master) MapTaskCompleted(taskID, workerID int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.MapTasks[taskID].State = Completed
	m.Workers[workerID].State = Idle
}

func (m *Master) ReduceTaskCompleted(taskID, workerID int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ReduceTasks[taskID].State = Completed
	m.Workers[workerID].State = Idle
}

func (m *Master) RequestTask(workerID int) Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, task := range m.MapTasks {
		if task.State == Pending {
			task.State = Running
			m.Workers[workerID].State = Busy
			return Task{task.ID, MapTaskType}
		}
	}

	for _, task := range m.MapTasks {
		if task.State != Completed {
			return Task{Type: WaitTaskType}
		}
	}

	for _, task := range m.ReduceTasks {
		if task.State == Pending {
			task.State = Running
			m.Workers[workerID].State = Busy
			return Task{task.ID, ReduceTaskType}
		}
	}

	return Task{Type: ExitTaskType}
}
