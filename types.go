package main

// Pair struct represent a key/value pair (k, v)
type Pair struct {
	key   string
	value int
}

type TaskState int

const (
	Pending TaskState = iota
	Running
	Completed
)

type MapTask struct {
	ID    int
	Words []string
	State TaskState
}

type WorkerState int

const (
	Idle WorkerState = iota
	Busy
)

type Worker struct {
	ID    int
	State WorkerState
}

type TaskResult struct {
	TaskID   int
	WorkerID int
}
