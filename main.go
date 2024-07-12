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

type Location struct {
	min   int16
	max   int16
	sum   int64
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
	loc.sum += int64(temp)
	loc.count += 1

	if temp < loc.min {
		loc.min = temp
	} else if temp > loc.max {
		loc.max = temp
	}
}

const chunkSize = 1024 * 1024
const workers = 32

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

	m := swiss.NewMap[string, *Location](825)

	buf := make([]byte, chunkSize)
	reader := bufio.NewReader(file)
	var leftData []byte

	chunkChan := make(chan []byte)
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			for chunk := range chunkChan {
				lines := bytes.Split(chunk, []byte{'\n'})
				for _, line := range lines {
					idx := 0
					if line[len(line)-5] == ';' {
						idx = len(line) - 5
					} else {
						idx = len(line) - 6
					}
					name := unsafe.String(unsafe.SliceData(line[:idx]), idx)
					temp := parse(line[idx+1:])

					loc, ok := m.Get(name)
					if !ok {
						loc = NewLocation()
						m.Put(name, loc)
					}
					loc.Add(temp)
				}
			}
			wg.Done()
		}()
	}

	var chunk []byte
	for {
		n, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		chunk = append(leftData, buf[:n]...)
		lastIndex := bytes.LastIndex(chunk, []byte{'\n'})
		leftData = chunk[lastIndex+1:]
		chunkChan <- chunk[:lastIndex]
	}
	close(chunkChan)

	wg.Wait()

	keys := make([]string, 0, m.Count())
	m.Iter(func(k string, _ *Location) (stop bool) {
		keys = append(keys, k)
		return false
	})

	sort.Strings(keys)

	for _, name := range keys {
		loc, _ := m.Get(name)
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
