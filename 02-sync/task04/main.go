// ============================================================
// Задача: Cyclic Barrier — повторяемый барьер  🔴 Senior
// ============================================================
//
// Вопрос с собесов уровня Senior.
//
// Барьер — примитив синхронизации: N горутин выполняют работу "фазами".
// Никакая горутина не переходит к следующей фазе пока все не завершили текущую.
//
//   type Barrier struct { ... }
//
//   func NewBarrier(n int) *Barrier
//   func (b *Barrier) Wait() // блокируется пока все n горутин не вызвали Wait
//
// В отличие от sync.WaitGroup:
//   - Многоразовый (cyclic): после прохода барьера все горутины стартуют снова
//   - Нельзя динамически менять счётчик
//
// Сценарий: параллельная симуляция. N воркеров выполняют шаг симуляции,
// потом все ждут на барьере, потом все вместе начинают следующий шаг.
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"sync"
	"time"
)

type Barrier struct {
	mu     sync.Mutex
	condon sync.Cond
	size   int
	count  int
	phase  int
}

// TODO: реализуй NewBarrier
func NewBarrier(n int) *Barrier {

	b := &Barrier{size: n}

	b.condon = *sync.NewCond(&b.mu)
	return b
}

// TODO: реализуй Wait
// Алгоритм:
//  1. Инкрементируй счётчик пришедших
//  2. Если это последняя горутина (count == n):
//     - сбрось count = 0
//     - поменяй phase
//     - вызови cond.Broadcast()
//  3. Иначе — жди (cond.Wait) пока phase не изменится
func (b *Barrier) Wait() {

	b.mu.Lock()
	b.count++
	curPhase := b.phase

	if b.count == b.size {
		b.count = 0
		b.phase++
		b.condon.Broadcast()
	} else {
		for b.phase == curPhase {
			b.condon.Wait()
		}
	}
	b.mu.Unlock()
}

func main() {
	const workers = 4
	const phases = 3

	barrier := NewBarrier(workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		id := i
		go func() {
			defer wg.Done()
			for phase := 0; phase < phases; phase++ {
				// Имитируем работу разной длины
				time.Sleep(time.Duration(50*(id+1)) * time.Millisecond)
				fmt.Printf("воркер %d завершил фазу %d\n", id, phase)

				barrier.Wait()
				fmt.Printf("воркер %d начинает фазу %d\n", id, phase+1)
			}
		}()
	}

	wg.Wait()
	fmt.Println("Все фазы завершены")
}
