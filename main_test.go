package main

import "testing"

func bytesToTemp1(b []byte) int16 {
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

func bytesToTemp2(b []byte) int16 {
	var v int16
	var isNeg int16 = 1

	for i := 0; i < len(b)-1; i++ {
		char := b[i]

		switch char {
		case '-':
			isNeg = -1
		case '.':
			digit := int16(b[i+1] - '0')
			v = v*10 + digit
		default:
			digit := int16(char - '0')
			v = v*10 + digit
		}
	}

	return v * isNeg
}

func bytesToTemp3(b []byte) int16 {
	var v int16
	var isNeg int16 = 1

	for i := 0; i < len(b); i++ {
		char := b[i]
		if char == '-' {
			isNeg = -1
		} else if char != '.' {
			digit := int16(char - '0')
			v = v*10 + digit
		}
	}

	return v * isNeg
}

func BenchmarkBytesToTemp1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bytesToTemp1([]byte("-23.4"))
	}
}

func BenchmarkBytesToTemp2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bytesToTemp2([]byte("-23.4"))
	}
}

func BenchmarkBytesToTemp3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bytesToTemp3([]byte("-23.4"))
	}
}

func parseLine1(line []byte) ([]byte, []byte) {
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

func parseLine2(line []byte) ([]byte, []byte) {
	idx := len(line) - 1

	// Iterate backwards to find the last occurrence of ';'
	for line[idx] != ';' {
		idx--
	}

	return line[:idx], line[idx+1:]
}

func BenchmarkParseLine1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseLine1([]byte("test;-23.4"))
	}
}

func BenchmarkParseLine2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseLine2([]byte("test;-23.4"))
	}
}
