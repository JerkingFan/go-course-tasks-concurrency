package main

import (
	"fmt"
	"strings"
	"sync"
)

type Task struct {
	ID   int
	Data string
}

func main() {

	tasks := make(chan Task)
	results := make(chan string)

	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(workerID int) {

			defer wg.Done()

			for task := range tasks {

				processed := strings.ToUpper(task.Data)
				result := fmt.Sprintf("Worker %d processed task %d: %s", workerID, task.ID, processed)

				results <- result

			}

		}(i)
	}

	go func() {
		// Создаём и отправляем 5 задач
		for i := 1; i <= 5; i++ {
			tasks <- Task{
				ID:   i,
				Data: fmt.Sprintf("task data %d", i),
			}
		}
		close(tasks) // закрываем канал задач, сигнализируя, что задач больше нет
	}()

	// Запускаем горутину для закрытия results после завершения всех воркеров
	go func() {
		wg.Wait()      // ждём завершения всех воркеров
		close(results) // закрываем канал результатов
	}()

	// Читаем и выводим результаты
	for result := range results {
		fmt.Println(result)
	}

	fmt.Println("Все задачи обработаны")

}
