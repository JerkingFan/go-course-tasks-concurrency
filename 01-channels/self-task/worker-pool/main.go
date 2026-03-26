package main

import (
	"fmt"
	"sync"
)

func main() {

	jobs := make(chan int)
	results := make(chan int)

	var wg sync.WaitGroup

	numbers := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	numSlaves := 3
	for i := 1; i <= numSlaves; i++ {
		wg.Add(1)
		go func() {

			defer wg.Done()

			for num := range jobs {

				results <- num * 2

			}

		}()
	}

	go func() {

		for _, num := range numbers {

			jobs <- num

		}
		close(jobs)
	}()

	// Запускаем горутину для закрытия results после завершения всех воркеров
	go func() {
		wg.Wait()      // ждём завершения всех воркеров
		close(results) // закрываем results, чтобы main мог выйти из range
	}()

	// Читаем и выводим результаты
	for result := range results {
		fmt.Println(result)
	}
}
