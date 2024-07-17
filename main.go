package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime/pprof"
	"sort"
	"sync"
	"unsafe"
)

const (
	citiesCount = 413
	chunkSize   = 2 * 1024 * 1024
	workers     = 12
)

type Location struct {
	min   int16
	max   int16
	sum   int32
	count uint32
}

func NewLocation() *Location {
	return &Location{
		min:   math.MaxInt16,
		max:   math.MinInt16,
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

	loc.sum += int32(temp)
	loc.count += 1
}

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

	chunkChan := make(chan []byte, workers)

	mapChan := make(chan map[string]*Location, workers)
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			lm := map[string]*Location{}
			for chunk := range chunkChan {
				start := 0
				for end, b := range chunk {
					if b != '\n' {
						continue
					}
					before, after := parseLine(chunk[start:end])
					name := unsafe.String(unsafe.SliceData(before), len(before))
					temp := bytesToTemp(after)

					loc, ok := lm[name]
					if !ok {
						loc = NewLocation()
						lm[name] = loc
					}
					loc.Add(temp)

					start = end + 1
				}
			}
			mapChan <- lm
			wg.Done()
		}()
	}

	buf := make([]byte, chunkSize)
	reader := bufio.NewReader(file)
	var leftData []byte
	for {
		n, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		chunk := append(leftData, buf[:n]...)
		lastIndex := bytes.LastIndex(chunk, []byte{'\n'})
		leftData = chunk[lastIndex+1:]
		chunkChan <- chunk[:lastIndex]
	}
	close(chunkChan)

	go func() {
		wg.Wait()
		close(mapChan)
	}()

	keys := make([]string, 0, 825)

	m := map[string]*Location{}
	for lm := range mapChan {
		for lk, lLoc := range lm {
			loc, ok := m[lk]
			if !ok {
				keys = append(keys, lk)
				m[lk] = lLoc
				continue
			}

			if lLoc.min < loc.min {
				loc.min = lLoc.min
			}
			if lLoc.max > loc.max {
				loc.max = lLoc.max
			}
			loc.sum += lLoc.sum
			loc.count += lLoc.count
		}
	}

	sort.Strings(keys)

	for _, name := range keys {
		loc := m[name]
		mean := float32(loc.sum) / float32(loc.count) / 10
		fmt.Printf("%s: %.1f/%.1f/%.1f\n", name, float32(loc.min)/10, mean, float32(loc.max)/10)
	}
}

func parseLine(line []byte) ([]byte, []byte) {
	idx := 0
	if line[len(line)-4] == ';' {
		idx = len(line) - 4
	} else if line[len(line)-5] == ';' {
		idx = len(line) - 5
	} else {
		idx = len(line) - 6
	}

	return line[:idx], line[idx+1:]
}

func bytesToTemp(b []byte) int16 {
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
