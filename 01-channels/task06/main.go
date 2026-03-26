// ============================================================
// Задача: Bounded Generator с Backpressure  🔴 Senior
// ============================================================
//
// Частый вопрос на собесах уровня Senior.
//
// Реализуй Generator — источник данных с ограниченным буфером.
// Если потребитель не успевает читать — Generator замедляется (backpressure).
//
// Интерфейс:
//
//   type Generator struct { ... }
//
//   func NewGenerator(bufSize int) *Generator
//   func (g *Generator) Send(v int) bool   // false если full и timeout
//   func (g *Generator) Chan() <-chan int
//   func (g *Generator) Close()
//
// Требования:
//   - Send блокируется максимум sendTimeout (100мс)
//   - Если за sendTimeout буфер не освободился — Send возвращает false
//   - Close завершает работу: Chan закрывается, Send возвращает false
//   - Безопасен для одновременного использования из нескольких горутин
//
// Дополнительно реализуй:
//   - Stats() (sent, dropped int64) — количество успешно отправленных и дропнутых
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

const sendTimeout = 100 * time.Millisecond

type Generator struct {
	out       chan int
	exit      chan struct{}
	wg        sync.WaitGroup // не ебу нахуя добавил, казалось, что это крутая идея, но типа не
	flagSend  atomic.Int64
	flagClose atomic.Int64
	once      sync.Once
}

// TODO: реализуй NewGenerator
func NewGenerator(bufSize int) *Generator {

	return &Generator{

		out:  make(chan int),
		exit: make(chan struct{}),
	}

}

// TODO: реализуй Send
// Отправляет значение в буферизованный канал.
// Если буфер полон — ждёт до sendTimeout, потом возвращает false.
// Если Generator закрыт — сразу возвращает false.
func (g *Generator) Send(v int) bool {

	//Оказывается гошка может в select выбрать блять любой case, типа если верны два кейса, гошка выберет любой, а не последовательно по цепочке, круто, спасибо папаша, когда патчноут?
	select {
	case <-g.exit:
		return false
	default:
	}

	select {

	case g.out <- v:
		g.flagSend.Add(1)
		return true
	case <-time.After(sendTimeout):
		g.flagClose.Add(1)
		return false
	case <-g.exit:
		g.flagClose.Add(1)
		return false
	}

}

// Chan возвращает канал для чтения данных
func (g *Generator) Chan() <-chan int {
	return g.out
}

// TODO: реализуй Close — закрой closed канал через sync.Once, закрой ch
func (g *Generator) Close() {

	g.once.Do(func() {

		close(g.out)
		close(g.exit)

	})
}

// Stats возвращает статистику
func (g *Generator) Stats() (flagSend, flagClose int64) {
	return g.flagSend.Load(), g.flagClose.Load()
}

func main() {
	gen := NewGenerator(5)

	// Медленный потребитель
	go func() {
		for v := range gen.Chan() {
			fmt.Printf("получено: %d\n", v)
			time.Sleep(50 * time.Millisecond)
		}
		fmt.Println("канал закрыт")
	}()

	// Быстрый производитель
	for i := 0; i < 20; i++ {
		ok := gen.Send(i)
		if !ok {
			fmt.Printf("дропнуто: %d\n", i)
		}
	}

	gen.Close()
	time.Sleep(500 * time.Millisecond)

	sent, dropped := gen.Stats()
	fmt.Printf("Отправлено: %d, Дропнуто: %d\n", sent, dropped)
}
