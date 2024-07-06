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
)

type Location struct {
	min   float32
	max   float32
	mean  float32
	count int
}

func NewLocation() *Location {
	return &Location{
		min:   999999,
		max:   -999999,
		mean:  0,
		count: 0,
	}
}

func (loc *Location) Add(temp float32) {
	loc.min = float32(math.Min(float64(loc.min), float64(temp)))
	loc.max = float32(math.Max(float64(loc.max), float64(temp)))
	loc.mean = (loc.mean*float32(loc.count) + float32(temp)) / (float32(loc.count) + 1)
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

	go func() {
		for {
			n, err := reader.Read(buf)
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}

			chunk := append(leftData, buf[:n]...)
			lines := bytes.Split(chunk, []byte{'\n'})
			lines, leftData = lines[:len(lines)-1], lines[len(lines)-1]
			linesChan <- lines
		}
		close(linesChan)
	}()

	for i := 0; i < workers; i++ {
		go func() {
			for lines := range linesChan {
				for _, line := range lines {
					before, after, _ := bytes.Cut(line, []byte{';'})
					name := string(before)
					temp := parse(after)

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
		fmt.Printf("%s: %.1f/%.1f/%.1f\n", name, loc.min, loc.mean, loc.max)
	}
}

func parse(b []byte) float32 {
	var v int32
	var isNeg int32 = 1

	for i := 0; i < len(b)-1; i++ {
		char := b[i]
		if char == '-' {
			isNeg = -1
		} else if char == '.' {
			digit := int32(b[i+1] - '0')
			v = v*10 + digit
		} else {
			digit := int32(char - '0')
			v = v*10 + digit
		}
	}

	return float32(v*isNeg) / 10
}
