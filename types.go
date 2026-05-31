package main

import "sync"

const (
	Pending TaskState = iota
	Running
	Completed
)

const (
	Idle WorkerState = iota
	Busy
)

// Pair struct represent a key/value pair (k, v)
type Pair struct {
	key   string
	value int
}

type Master struct {
	mu          sync.Mutex
	MapTasks    map[int]*MapTask
	ReduceTasks map[int]*ReduceTask
	Workers     map[int]*Worker
}

type (
	TaskState   int
	WorkerState int
)

type MapTask struct {
	ID    int
	State TaskState
	Words []string
}

type ReduceTask struct {
	ID        int
	State     TaskState
	Partition int
}

type Worker struct {
	ID    int
	State WorkerState
}

type TaskType int

const (
	MapTaskType TaskType = iota
	ReduceTaskType
	WaitTaskType
	ExitTaskType
)

type Task struct {
	TaskID int
	Type   TaskType
}
