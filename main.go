package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"unicode"
)

type Pair struct {
	key   string
	value int
}

func Map(data string) Pair {
	return Pair{
		key:   data,
		value: 1,
	}
}

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
	var mapWg sync.WaitGroup
	var partitionWg sync.WaitGroup
	var reducerWg sync.WaitGroup
	pairChan := make(chan Pair)

	reducer := 3
	reducerChan := make([]chan Pair, reducer)

	entries, err := os.ReadDir("data")
	if err != nil {
		fmt.Println("Error reading directory.")
		return
	}

	// Shuffle, grouping, and reduce

	partitionWg.Add(1)
	reducerWg.Add(reducer)

	for i := range reducer {
		reducerChan[i] = make(chan Pair)
		ch := reducerChan[i]
		go func(ch chan Pair) {
			defer reducerWg.Done()
			grouped := make(map[string][]int)

			for pair := range ch {
				grouped[pair.key] = append(grouped[pair.key], pair.value)
			}

			for key, value := range grouped {
				result := Reduce(key, value)
				fmt.Printf("(%s, %d)\n", result.key, result.value)
			}
		}(ch)
	}

	go func() {
		defer partitionWg.Done()
		for pair := range pairChan {
			partition := int(Hash(pair.key)) % reducer
			reducerChan[partition] <- pair
		}
		for _, ch := range reducerChan {
			close(ch)
		}
	}()

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
			mapWg.Add(1)
			go func(chunk []string) {
				defer mapWg.Done()
				for _, word := range chunk {
					pairChan <- Map(word)
				}
			}(chunk)
		}
	}

	mapWg.Wait()
	close(pairChan)

	partitionWg.Wait()
	reducerWg.Wait()

	// Reduce
}
