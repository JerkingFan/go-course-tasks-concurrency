package main

import "fmt"

func producer(nums []int) <-chan int {

	buf_ch := make(chan int, 3)

	go func() {

		for _, i := range nums {

			buf_ch <- i

		}
		close(buf_ch)

	}()
	return buf_ch
}

func main() {

	numbers := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	ch := producer(numbers)

	for val := range ch {
		fmt.Println(val)
	}

}
