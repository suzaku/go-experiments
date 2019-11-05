package test

import (
	"testing"
	"time"
)

const (
	produceLatency = 10 * time.Microsecond
	processLatency = 500 * time.Microsecond
)

// simulate a slow producer
func producer(n int, buf chan<- int) {
	for i := 0; i < n; i++ {
		time.Sleep(produceLatency)
		buf <- i
	}
}

var count int

// simulate a batch processing that take some overhead
func process(batch []int) {
	time.Sleep(processLatency)
	count += len(batch)
}

func take1(n int) {
	buf := make(chan int, 1>>20)
	go func() {
		producer(n, buf)
		close(buf)
	}()

	var batch []int
	for {
		select {
		case i, ok := <-buf:
			if !ok {
				process(batch)
				return
			}
			batch = append(batch, i)
			if len(batch) >= 500 {
				process(batch)
				batch = batch[:0]
			}
		default:
			if len(batch) > 0 {
				process(batch)
				batch = batch[:0]
				continue
			}
			i, ok := <-buf
			if !ok {
				return
			}
			batch = append(batch, i)
		}
	}
}

func take2(n int) {
	buf := make(chan int, 1>>20)
	go func() {
		producer(n, buf)
		close(buf)
	}()

	var batch []int
	for {
		select {
		case i, ok := <-buf:
			if !ok {
				process(batch)
				return
			}
			batch = append(batch, i)
			if len(batch) >= 500 {
				process(batch)
				batch = batch[:0]
			}
		case <-time.After(40 * time.Microsecond):
			if len(batch) > 0 {
				process(batch)
				batch = batch[:0]
				continue
			}
			i, ok := <-buf
			if !ok {
				return
			}
			batch = append(batch, i)
		}
	}
}

func take3(n int) {
	buf := make(chan int, 1>>20)
	go func() {
		producer(n, buf)
		close(buf)
	}()

	var batch []int
	ticker := time.NewTicker(time.Microsecond * 500)
	defer ticker.Stop()
	for {
		select {
		case i, ok := <-buf:
			if !ok {
				process(batch)
				return
			}
			batch = append(batch, i)
			if len(batch) >= 500 {
				process(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				process(batch)
				batch = batch[:0]
			}
		}
	}
}

func BenchmarkTake1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		count = 0
		take1(5000)
		if count != 5000 {
			b.Fatal("Incorrect")
		}
	}
}

func BenchmarkTake2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		count = 0
		take2(5000)
		if count != 5000 {
			b.Fatal("Incorrect")
		}
	}
}

func BenchmarkTake3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		count = 0
		take3(5000)
		if count != 5000 {
			b.Fatal("Incorrect")
		}
	}
}
