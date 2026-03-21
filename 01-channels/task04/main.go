// ============================================================
// Задача: Timeout & Select — первый ответ выигрывает  🟡 Middle
// ============================================================
//
// Реализуй функцию fastest(ctx, urls) которая:
//   - Параллельно запрашивает все переданные URL (mock)
//   - Возвращает первый успешный ответ
//   - Отменяет остальные запросы
//   - Возвращает ошибку если все запросы упали или истёк таймаут ctx
//
// Это классический паттерн "hedged requests" широко используемый в продакшне.
//
// Дополнительно реализуй withTimeout(d, fn):
//   - Запускает fn с таймаутом d
//   - Если fn не завершилась за d — возвращает ErrTimeout
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

type ResultChan struct {
	val string
	err string
}

var ErrTimeout = errors.New("таймаут истёк")
var ErrAllFailed = errors.New("все запросы завершились ошибкой")

type Result struct {
	URL  string
	Body string
}

// mockFetch имитирует HTTP-запрос с случайной задержкой
func mockFetch(ctx context.Context, url string) (Result, error) {
	delay := time.Duration(50+rand.Intn(200)) * time.Millisecond

	select {
	case <-time.After(delay):
		if rand.Float64() < 0.2 { // 20% вероятность ошибки
			return Result{}, fmt.Errorf("%s: server error", url)
		}
		return Result{URL: url, Body: fmt.Sprintf("response from %s", url)}, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

// TODO: реализуй fastest
// Алгоритм:
//  1. Для каждого url запусти горутину с mockFetch
//  2. Через select жди первый успешный результат
//  3. При получении — отмени контекст (остальные сами остановятся)
//  4. Если все вернули ошибку — вернуть ErrAllFailed
//  5. Если ctx отменён раньше — вернуть ErrTimeout
func fastest(ctx context.Context, urls []string) (Result, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	result := make(chan Result)
	err := make(chan error)

	for _, url := range urls {

		go func(s string) {
			res, errors := mockFetch(ctx, s)

			if errors != nil {

				select {
				case err <- errors:
				case <-ctx.Done():

				}
				return

			}
			select {

			case result <- res:
			case <-ctx.Done():

			}

		}(url)

	}

	for i := 0; i < len(urls); i++ {

		select {

		case <-ctx.Done():
			return Result{}, ErrTimeout
		case resulta := <-result:
			return resulta, nil
		case <-err:

		}

	}

	return Result{}, ErrAllFailed
}

// TODO: реализуй withTimeout
func withTimeout(d time.Duration, fn func() (string, error)) (string, error) {

	resultChan := make(chan struct {
		val string
		err error
	}, 1)

	go func() {

		val, err := fn()
		resultChan <- struct {
			val string
			err error
		}{val, err}
		time.After(d)

	}()

	select {
	case <-time.After(d):
		return "", ErrTimeout
	case res := <-resultChan:
		return res.val, res.err
	}

}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	urls := []string{
		"https://api1.example.com",
		"https://api2.example.com",
		"https://api3.example.com",
	}

	result, err := fastest(ctx, urls)
	if err != nil {
		fmt.Println("Ошибка:", err)
		return
	}
	fmt.Printf("Быстрейший ответ от %s: %s\n", result.URL, result.Body)
}
