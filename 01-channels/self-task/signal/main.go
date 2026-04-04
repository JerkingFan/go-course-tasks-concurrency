package main

import (
	"fmt"
	"time"
)

// process имитирует "тяжёлую" работу: удваивает число
func process() bool {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := 0; n < 7; n++ {
			time.Sleep(100 * time.Millisecond) // имитация работы
			result := n * 2
			fmt.Printf("process: %d -> %d\n", n, result)
			out <- result
		}
	}()
	return true
}

func main() {

	done := make(chan int)
	numSlaves := 5

	for i := 0; i < numSlaves; i++ {

		go func(id int) {
			if process() {

				done <- i

			}
		}(i)
	}
	// 4. Ждём завершения всех горутин через канал
	completed := 0
	for completed < numSlaves {
		id := <-done
		completed++
		fmt.Printf("Получен сигнал от горутины %d. Завершено: %d/%d\n", id, completed, numSlaves)

	}

	fmt.Println("Селяви брат")

}
