// ============================================================
// Задача: Worker Pool  🟡 Middle
// ============================================================
//
// Один из самых частых вопросов на собесах в любой Go-компании.
//
// Реализуй WorkerPool:
//
//   type WorkerPool struct { ... }
//
//   func NewWorkerPool(workers int) *WorkerPool
//   func (p *WorkerPool) Submit(task func()) bool  // false если пул остановлен
//   func (p *WorkerPool) Stop()                    // graceful: ждёт завершения текущих задач
//   func (p *WorkerPool) StopNow()                 // немедленная остановка
//   func (p *WorkerPool) Running() int             // количество активных воркеров
//
// Требования:
//   - Фиксированный пул из workers горутин
//   - Submit не блокируется (очередь задач буферизована, размер 100)
//   - Stop ждёт пока все отправленные задачи завершатся
//   - StopNow отменяет незапущенные задачи, ждёт текущие
//   - Нет утечек горутин
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type WorkerPool struct {
	jobs    chan func()
	wg      sync.WaitGroup
	once    sync.Once
	running atomic.Int32
}

// TODO: реализуй NewWorkerPool
func NewWorkerPool(workers int) *WorkerPool {

	slaves := &WorkerPool{
		jobs: make(chan func()),
	}

	for range workers {

		slaves.wg.Add(1)
		go func() {
			defer slaves.wg.Done()
			for task := range slaves.jobs {
				task()
			}

		}()

	}

	slaves.running.Store(1)

	return slaves
}

// TODO: реализуй Submit
func (p *WorkerPool) Submit(task func()) bool {

	select {
	case p.jobs <- task:
		return true
	default:
		return false
	}

}

// Stop ждёт завершения всех задач
func (slaves *WorkerPool) Stop() {

	slaves.once.Do(func() {
		close(slaves.jobs)
	})
	slaves.wg.Wait()

}

// StopNow немедленно закрывает канал, дропает незапущенные задачи
func (slaves *WorkerPool) StopNow() {

	slaves.once.Do(func() {
		for {

			select {
			case <-slaves.jobs:
			default:
				close(slaves.jobs)
				return
			}

		}
	})
	slaves.wg.Wait()

}

func (p *WorkerPool) Running() int {
	return int(p.running.Load())
}

func main() {
	pool := NewWorkerPool(3)

	var mu sync.Mutex
	var results []int

	for i := 0; i < 10; i++ {
		n := i
		pool.Submit(func() {
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			results = append(results, n)
			mu.Unlock()
			fmt.Printf("задача %d выполнена\n", n)
		})
	}

	pool.Stop()
	fmt.Printf("Выполнено задач: %d\n", len(results))
}
