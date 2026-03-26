package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

func worker(ctx context.Context, ch chan<- int) {

	for {

		select {
		case <-ctx.Done():
			close(ch)
			return
		default:

			ch <- rand.Intn(100)
			time.Sleep(500 * time.Millisecond)
		}

	}

}

func main() {
	// Создаём контекст с таймаутом 3 секунды
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel() // хорошая практика: отменять контекст при выходе

	// Создаём канал
	ch := make(chan int)

	// Запускаем воркер
	go worker(ctx, ch)

	// Читаем из канала, пока он открыт
	for num := range ch {
		fmt.Printf("Получено: %d\n", num)
	}

	fmt.Println("Программа завершена")
}
