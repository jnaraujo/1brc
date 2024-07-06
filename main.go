package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"sort"
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

func main() {
	file, _ := os.Open("./measurements.txt")
	defer file.Close()
	scanner := bufio.NewScanner(file)

	m := map[string]*Location{}
	for scanner.Scan() {
		before, after, _ := bytes.Cut(scanner.Bytes(), []byte{';'})
		name := string(before)
		temp := parse(after)

		loc, ok := m[name]
		if !ok {
			loc = NewLocation()
			m[name] = loc
		}
		loc.Add(temp)
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

func parse(b []byte) float32 {
	v := float32(0)

	isNeg := 1
	for _, char := range b {
		if char != '.' {
			v *= 10
		}

		switch char {
		case '-':
			isNeg = -1
		case '1':
			v += 1
		case '2':
			v += 2
		case '3':
			v += 3
		case '4':
			v += 4
		case '5':
			v += 5
		case '6':
			v += 6
		case '7':
			v += 7
		case '8':
			v += 8
		case '9':
			v += 9
		}
	}
	return v * float32(isNeg) / 10
}
