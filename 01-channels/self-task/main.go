package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

type OutVal struct {
	val int
	err error
}

var errTimeoute = errors.New("Suck my dick ")

func processData(ctx context.Context, val int) chan OutVal {

	ch := make(chan struct{})
	out := make(chan OutVal, 1)

	go func() {
		time.Sleep(time.Duration(rand.IntN(10)) * time.Second)
		close(ch)
	}()

	select {
	case <-ch:
		out <- OutVal{

			val: val * 2,
			err: nil,
		}
	case <-ctx.Done():
		out <- OutVal{

			val: 0,
			err: errTimeoute,
		}
	}
	close(out)
	return out

}

func main() {

	in := make(chan int)
	out := make(chan int)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	go func() {

		for i := range 10 {

			in <- i

		}

		close(in)
	}()

	now := time.Now()
	processParallel(ctx, in, out, 5)

	for val := range out {

		fmt.Println(val)

	}
	fmt.Println(time.Since(now))

}

func processParallel(ctx context.Context, in <-chan int, out chan<- int, numWorkers int) {

	var wg sync.WaitGroup

	for range numWorkers {

		wg.Add(1)
		go func() {

			defer wg.Done()

			for v := range in {

				select {

				case res := <-processData(ctx, v):
					if res.err != nil {

						return

					}
					out <- res.val
				case <-ctx.Done():
					return

				}

			}

		}()

	}

	go func() {

		wg.Wait()
		close(out)

	}()

}
