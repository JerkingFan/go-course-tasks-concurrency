package main

import (
	"fmt"
	"sync"
)

func main() {

	ch := make(chan int)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {

		for i := 0; i <= 9; i++ {

			ch <- i

		}
		close(ch)
	}()

	for u := range ch {

		fmt.Println(u)

	}

	wg.Wait()

}
