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

	"github.com/dolthub/swiss"
)

const (
	chunkSize = 1024 * 1024
	workers   = 12
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
	mapChan := make(chan *swiss.Map[string, *Location], workers)
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			lm := swiss.NewMap[string, *Location](825)
			for chunk := range chunkChan {
				lines := bytes.Split(chunk, []byte{'\n'})
				for _, line := range lines {
					before, after := parseLine(line)
					name := unsafe.String(unsafe.SliceData(before), len(before))
					temp := bytesToTemp(after)

					loc, ok := lm.Get(name)
					if !ok {
						loc = NewLocation()
						lm.Put(name, loc)
					}
					loc.Add(temp)
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

	m := swiss.NewMap[string, *Location](825)
	for lm := range mapChan {
		lm.Iter(func(lk string, lLoc *Location) (stop bool) {
			loc, ok := m.Get(lk)
			if !ok {
				keys = append(keys, lk)
				m.Put(lk, lLoc)
				return false
			}

			if lLoc.min < loc.min {
				loc.min = lLoc.min
			}
			if lLoc.max > loc.max {
				loc.max = lLoc.max
			}
			loc.sum += lLoc.sum
			loc.count += lLoc.count

			return false
		})
	}

	sort.Strings(keys)

	for _, name := range keys {
		loc, _ := m.Get(name)
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
