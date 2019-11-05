package test

import (
	"testing"
	"time"
)

// simulate a slow producer
func producer(n int, buf chan<- int) {
	for i := 0; i < n; i++ {
		time.Sleep(10 * time.Microsecond)
		buf <- i
	}
}

var count int

// simulate a batch processing that take some overhead
func process(batch []int) {
	time.Sleep(500 * time.Microsecond)
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
