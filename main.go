package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"sort"
	"sync"
)

type Location struct {
	min   int16
	max   int16
	sum   int64
	count uint32
}

func NewLocation() *Location {
	return &Location{
		min:   9999,
		max:   -9999,
		sum:   0,
		count: 0,
	}
}

func (loc *Location) Add(temp int16) {
	if temp < loc.min {
		loc.min = temp
	}
	if temp > loc.max {
		loc.max = temp
	}
	loc.sum += int64(temp)
	loc.count += 1
}

const chunkSize = 50 * 1024 * 1024
const workers = 8

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	file, _ := os.Open("./measurements.txt")
	defer file.Close()

	m := map[string]*Location{}

	buf := make([]byte, chunkSize)
	reader := bufio.NewReader(file)
	var leftData []byte

	linesChan := make(chan [][]byte)
	var wg sync.WaitGroup
	wg.Add(workers)

	var chunk []byte
	var lines [][]byte

	go func() {
		for {
			n, err := reader.Read(buf)
			if err != nil {
				if err == io.EOF {
					// last chunk
					if len(leftData) > 0 {
						linesChan <- [][]byte{leftData}
					}
					break
				}
				panic(err)
			}

			chunk = append(leftData, buf[:n]...)
			lines = bytes.Split(chunk, []byte{'\n'})
			lines, leftData = lines[:len(lines)-1], lines[len(lines)-1]
			linesChan <- lines
		}
		close(linesChan)
	}()

	for i := 0; i < workers; i++ {
		go func() {
			for lines := range linesChan {
				for _, line := range lines {
					idx := 0
					if line[len(line)-5] == ';' {
						idx = len(line) - 5
					} else {
						idx = len(line) - 6
					}
					name := string(line[:idx])
					temp := parse(line[idx+1:])

					loc, ok := m[name]
					if !ok {
						loc = NewLocation()
						m[name] = loc
					}
					loc.Add(temp)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, name := range keys {
		loc := m[name]
		mean := float32(loc.sum) / float32(loc.count) / 10
		fmt.Printf("%s: %.1f/%.1f/%.1f\n", name, float32(loc.min)/10, mean, float32(loc.max)/10)
	}
}

func parse(b []byte) int16 {
	var v int16
	var isNeg int16 = 1

	for i := 0; i < len(b)-1; i++ {
		char := b[i]
		if char == '-' {
			isNeg = -1
		} else if char == '.' {
			digit := int16(b[i+1] - '0')
			v = v*10 + digit
		} else {
			digit := int16(char - '0')
			v = v*10 + digit
		}
	}

	return v * isNeg
}
