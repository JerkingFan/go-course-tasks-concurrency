// ============================================================
// Задача: Взвешенный семафор  🟡 Middle
// ============================================================
//
// Вопрос с собесов уровня Middle.
//
// Реализуй Semaphore с поддержкой "веса" (weighted semaphore):
//   - Ресурс имеет ёмкость N
//   - Acquire(n) захватывает n единиц. Блокируется если доступно < n.
//   - Release(n) возвращает n единиц.
//   - TryAcquire(n) — non-blocking: захватывает или возвращает false
//
// Примеры использования:
//   - Ограничение числа параллельных HTTP-запросов (каждый = 1 единица)
//   - Управление памятью (запрос на 10МБ = 10 единиц)
//   - Rate limiting по "стоимости" операции
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

type Semaphore struct {
	ch chan struct{}
}

// NewSemaphore создаёт семафор с ёмкостью n.
func NewSemaphore(n int) (*Semaphore, error) {

	//Не знаю насколько критична данная проверка, но просто типа мало ли + я типа декларацию функции изменил, а следовательно по пизде пошли некоторые моменты, поэтому типа хз, насколько правильно
	if n < 0 {
		return nil, errors.New("Отрицательная емкость? Хм, знаешь, что еще отрицательное? Состав гастомельского десанта")
	}

	chanel := make(chan struct{}, n)

	for range n {
		chanel <- struct{}{}
	}
	return &Semaphore{ch: chanel}, nil
}

// Acquire блокирующий захват n единиц.
// TODO: реализуй через цикл с чтением из ch
func (s *Semaphore) Acquire(n int) {

	for range n {
		<-s.ch
	}

}

// AcquireContext захват с контекстом — можно отменить.
// TODO: реализуй — если ctx отменён до получения всех n единиц,
//
//	верни уже захваченные обратно и вернуть ctx.Err()
func (s *Semaphore) AcquireContext(ctx context.Context, n int) error {

	collect := 0

	for range n {

		select {

		case <-s.ch:
			collect++
		case <-ctx.Done():
			s.Release(collect)
			return ctx.Err()

		}

	}
	return nil
}

// TryAcquire non-blocking захват. Возвращает false если доступно < n.
// TODO: реализуй
func (s *Semaphore) TryAcquire(n int) bool {
	collect := 0

	for range n {

		select {

		case <-s.ch:
			collect++
		default:
			if collect < n {
				return false
			} else {
				s.Release(collect)
				return true
			}

		}
	}
	return false
}

// Release возвращает n единиц.
func (s *Semaphore) Release(n int) {

	for range n {
		s.ch <- struct{}{}
	}
}

// Available возвращает количество свободных единиц.
func (s *Semaphore) Available() int {
	return len(s.ch)
}

func main() {
	sem, err := NewSemaphore(3)
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		n := i
		go func() {
			defer wg.Done()
			sem.Acquire(1)
			defer sem.Release(1)
			fmt.Printf("задача %d выполняется (доступно: %d)\n", n, sem.Available())
			time.Sleep(100 * time.Millisecond)
		}()
	}
	wg.Wait()
}
