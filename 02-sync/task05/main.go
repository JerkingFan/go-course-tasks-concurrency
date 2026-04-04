// ============================================================
// Задача: Очередь с ожиданием через sync.Cond  🔴 Senior
// ============================================================
//
// Вопрос с собесов уровня Senior.
//
// Реализуй блокирующую очередь (blocking queue):
//
//   type BlockingQueue[T any] struct { ... }
//
//   func NewBlockingQueue[T any](capacity int) *BlockingQueue[T]
//   func (q *BlockingQueue[T]) Put(item T)   // блокируется если очередь полна
//   func (q *BlockingQueue[T]) Take() T      // блокируется если очередь пуста
//   func (q *BlockingQueue[T]) PutTimeout(item T, d time.Duration) bool
//   func (q *BlockingQueue[T]) TakeTimeout(d time.Duration) (T, bool)
//   func (q *BlockingQueue[T]) Len() int
//   func (q *BlockingQueue[T]) Close()       // пробуждает все заблокированные горутины
//
// Используй sync.Cond (не каналы!).
//
// Зачем sync.Cond вместо каналов?
//   - Каналы имеют фиксированный тип и не поддерживают broadcast
//   - sync.Cond позволяет гибко управлять условиями пробуждения
//   - Используется в стандартной библиотеке (sync.Pool, etc.)
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"sync"
	"time"
)

type BlockingQueue[T any] struct {
	arr      []T
	capacity int
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	close    bool
}

// TODO: реализуй NewBlockingQueue
func NewBlockingQueue[T any](capacity int) *BlockingQueue[T] {

	if capacity < 0 {
		panic("АЙ СЫН ША ОТРИЦАТЕЛЬНАЯ ЁМКОСТЬ")
	}

	bq := &BlockingQueue[T]{
		arr:      make([]T, 0, capacity),
		capacity: capacity,
		close:    false,
	}
	bq.notEmpty = sync.NewCond(&bq.mu)
	bq.notFull = sync.NewCond(&bq.mu)

	return bq

}

// TODO: реализуй Put — блокируется пока len(items) == cap
func (bq *BlockingQueue[T]) Put(item T) {

	bq.mu.Lock()
	if bq.close {
		bq.mu.Unlock()
		return
	}
	for len(bq.arr) == bq.capacity {
		bq.notFull.Wait()
	}
	bq.arr = append(bq.arr, item)
	bq.notEmpty.Signal()
	bq.mu.Unlock()

}

// TODO: реализуй Take — блокируется пока len(items) == 0
func (bq *BlockingQueue[T]) Take() (zero T) {

	bq.mu.Lock()
	defer bq.mu.Unlock()
	for len(bq.arr) == 0 {
		bq.notEmpty.Wait()
	}

	item := bq.arr[0]
	bq.arr = bq.arr[1:]

	return item
}

// TODO: реализуй PutTimeout
// Подсказка: запусти горутину с таймером которая вызывает notFull.Broadcast()
func (bq *BlockingQueue[T]) PutTimeout(item T, d time.Duration) bool {

	bq.mu.Lock()
	defer bq.mu.Unlock()
	if bq.close == true {
		return false
	}
	timeout := make(chan struct{})

	go func() {
		time.Sleep(d)
		bq.notFull.Broadcast()
		close(timeout)
	}()

	for len(bq.arr) == bq.capacity {

		bq.notFull.Wait()
		select {
		case <-timeout:
			return false
		default:

		}

	}
	bq.arr = append(bq.arr, item)
	bq.notEmpty.Signal()
	return true
}

// TODO: реализуй TakeTimeout аналогично
func (bq *BlockingQueue[T]) TakeTimeout(d time.Duration) (zero T, ok bool) {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	if bq.close {
		return zero, false
	}

	timeout := make(chan struct{})

	go func() {
		time.Sleep(d)
		bq.notEmpty.Broadcast()
		close(timeout)
	}()

	for len(bq.arr) == 0 {
		bq.notEmpty.Wait()
		select {
		case <-timeout:
			return zero, false
		default:
		}
	}

	item := bq.arr[0]
	bq.arr = bq.arr[1:]

	bq.notFull.Signal()

	return item, true
}

func (q *BlockingQueue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.arr)
}

// Close закрывает очередь и пробуждает все заблокированные горутины
func (q *BlockingQueue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.close = true
	q.notFull.Broadcast()
	q.notEmpty.Broadcast()
}

func main() {
	q := NewBlockingQueue[int](3)

	// Производитель
	go func() {
		for i := 0; i < 10; i++ {
			q.Put(i)
			fmt.Printf("Положено: %d, в очереди: %d\n", i, q.Len())
			time.Sleep(50 * time.Millisecond)
		}
		q.Close()
	}()

	// Потребитель (медленный)
	for {
		v, ok := q.TakeTimeout(5000 * time.Millisecond)
		if !ok {
			fmt.Println("Очередь закрыта или таймаут")
			break
		}
		fmt.Printf("Взято: %d\n", v)
		time.Sleep(100 * time.Millisecond)
	}
}
