package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
)

type Location struct {
	min   float32
	max   float32
	mean  float32
	count int
}

func main() {
	file, _ := os.Open("./measurements.txt")
	defer file.Close()
	scanner := bufio.NewScanner(file)

	m := map[string]*Location{}
	for scanner.Scan() {
		before, after, _ := bytes.Cut(scanner.Bytes(), []byte{';'})
		name := string(before)
		temp, _ := strconv.ParseFloat(string(after), 32)

		loc, ok := m[name]
		if !ok {
			loc = &Location{
				min:   999999,
				max:   -999999,
				mean:  0,
				count: 0,
			}
		}

		loc.min = float32(math.Min(float64(loc.min), float64(temp)))
		loc.max = float32(math.Max(float64(loc.max), float64(temp)))
		loc.mean = (loc.mean*float32(loc.count) + float32(temp)) / (float32(loc.count) + 1)
		loc.count += 1

		m[name] = loc
	}

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
