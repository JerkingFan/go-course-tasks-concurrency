package main

import (
	"fmt"
	"time"
)

func main() {

	ch1 := make(chan int)
	ch2 := make(chan int)

	// Горутина для первого канала
	go func() {
		for i := 1; ; i++ {
			ch1 <- i
			time.Sleep(1 * time.Second) // интервал 1 секунда
		}
	}()

	// Горутина для второго канала
	go func() {
		for i := 1; ; i++ {
			ch2 <- i
			time.Sleep(2 * time.Second) // интервал 2 секунды
		}
	}()

	timeout := time.After(10 * time.Second)

	for {
		select {
		case val1 := <-ch1:
			fmt.Printf("Из канала 1: %d\n", val1)
		case val2 := <-ch2:
			fmt.Printf("Из канала 2: %d\n", val2)
		case <-timeout:
			fmt.Println("Время вышло, завершаем программу")
			return
		}
	}

}
