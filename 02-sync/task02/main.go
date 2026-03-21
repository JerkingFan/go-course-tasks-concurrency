// ============================================================
// Задача: sync.Once — ленивая инициализация и Singleton  🟢 Junior
// ============================================================
//
// Три реализации одного паттерна — найди правильную:
//
// Вариант A: глобальная переменная без синхронизации     → БАГИ, найди их
// Вариант B: sync.Mutex на каждом Get                    → работает, но медленно
// Вариант C: sync.Once                                   → РЕАЛИЗУЙ
// Вариант D: double-checked locking (антипаттерн в Go)   → БАГИ, найди их
//
// Задача 1: Объясни почему Вариант A и D содержат гонки.
// Задача 2: Реализуй Вариант C (sync.Once).
// Задача 3: Реализуй onceWithError — sync.Once который умеет возвращать ошибку.
//           Если инициализация упала с ошибкой — следующий вызов повторяет её.
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"sync"
)

// === Версия A: НЕПРАВИЛЬНАЯ — гонка ===
var globalDB *MockDB

// Ну типа есть две горутины, которые видят, что выполняется условие и такие "Михалыч, тут нил, пошли работать" и вот они пойдут работать. Возможно такая ситуация, что эта переменная
// отвечает типа за вызов ядерных боеголовок по Вашингтону и типа если стоит нил и какое-то внешнее условие, то надо установить значение чтобы запустить ракеты
// и вот две горутины пошли включать ракеты, велик шанс, что один включит а другой выключит
func GetDB_Broken() *MockDB {
	muDB.Lock()
	defer muDB.Unlock()
	if globalDB == nil { // ← гонка: несколько горутин могут пройти это условие
		globalDB = NewMockDB()
	}
	return globalDB
}

// === Версия B: Mutex — правильно, но медленно ===
var (
	muDB   sync.Mutex
	dbOnce *MockDB
)

func GetDB_Mutex() *MockDB {
	muDB.Lock()
	defer muDB.Unlock()
	if dbOnce == nil {
		dbOnce = NewMockDB()
	}
	return dbOnce
}

// === Версия C: sync.Once — РЕАЛИЗУЙ ===
var (
	onceDB   sync.Once
	singleDB *MockDB
)

// TODO: реализуй GetDB_Once
func GetDB_Once() *MockDB {
	// TODO: используй onceDB.Do(func() { singleDB = NewMockDB() })

	onceDB.Do(func() {

		singleDB = NewMockDB()

	})

	return singleDB
}

// === Задача 3: Once с обработкой ошибки ===

// OnceWithError — как sync.Once, но запоминает ошибку.
// Если fn вернула ошибку — следующий вызов Do снова вызывает fn.
// Если fn успешна — все последующие вызовы возвращают кешированный результат.
type OnceWithError struct {
	mu   sync.Mutex
	done bool
	val  any
	err  error
}

// TODO: реализуй Do
func (o *OnceWithError) Do(fn func() (any, error)) (any, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.done {
		return o.val, o.err
	}

	val, err := fn()

	if err == nil {
		o.done = true
	}

	o.val = val
	o.err = err

	// TODO: вызови fn()
	// TODO: если err == nil — установи done = true
	// TODO: сохрани val и err

	return val, err
}

// === Вспомогательный мок ===

type MockDB struct{ id int }

var dbCounter int
var dbMu sync.Mutex

func NewMockDB() *MockDB {
	dbMu.Lock()
	defer dbMu.Unlock()
	dbCounter++
	fmt.Printf("Создан MockDB #%d\n", dbCounter)
	return &MockDB{id: dbCounter}
}

func main() {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db := GetDB_Once()
			_ = db
		}()
	}
	wg.Wait()

	fmt.Printf("Всего создано DB: %d (ожидаем 1)\n", dbCounter)
}
