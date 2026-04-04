// ============================================================
// Задача: Планировщик задач с приоритетами  ⚫ Expert
// ============================================================
//
// Вопрос с финальных этапов собеса уровня Staff+.
//
// Реализуй Scheduler — планировщик с:
//   - Приоритетами (High > Medium > Low)
//   - Отменой через context
//   - Зависимостями: задача B стартует только после завершения задачи A
//   - Дедлайнами: задача отменяется если не стартовала до дедлайна
//   - Метриками: время ожидания, время выполнения
//
//   type Scheduler struct { ... }
//
//   func NewScheduler(workers int) *Scheduler
//   func (s *Scheduler) Schedule(task Task) TaskID
//   func (s *Scheduler) Cancel(id TaskID) bool
//   func (s *Scheduler) Wait(id TaskID) error
//   func (s *Scheduler) Shutdown()
//   func (s *Scheduler) Stats() Stats
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Priority int

const (
	PriorityHigh   Priority = 3
	PriorityMedium Priority = 2
	PriorityLow    Priority = 1
)

type TaskID int64

type TaskStatus int
const (
    StatusPending TaskStatus = iota
    StatusReady
    StatusRunning
    StatusCompleted
    StatusFailed
    StatusCanceled
    StatusTimedOut
)

//Хуйня для создания таски и передачи в планировщик
type Task struct {
    Fn           func(ctx context.Context) error
    Priority     int                     // чем меньше число, тем выше приоритет
    Dependencies []TaskID               
    Deadline     time.Time              
   
}

//Хуйня для отслеживания положнякак с задачей
type taskState struct {
    ID             TaskID
    Priority       int
    Fn             func(ctx context.Context) error
    Ctx            context.Context
    Cancel         context.CancelFunc
    Dependencies   []TaskID
    RemainingDeps  int                     
    Dependents     []TaskID               
    Status         TaskStatus              // enum: pending, ready, running, completed, failed, canceled, timedout
    Err            error
    CreatedAt      time.Time
    StartedAt      time.Time
    FinishedAt     time.Time
    WaitCh         chan struct{}           // канал, на котором ждут в Wait()
    Deadline       time.Time              
}

type Stats struct {
    Submitted  atomic.Int64
    Completed  atomic.Int64
    Failed     atomic.Int64
    Canceled   atomic.Int64
    TimedOut   atomic.Int64
    TotalWait  atomic.Int64 
    TotalExec  atomic.Int64 
}

type Scheduler struct {
    workers   int
    mu        sync.Mutex
    cond      *sync.Cond               // условная переменная для пробуждения воркеров
    ready     [PriorityLevels][]*taskState // очередь готовых задач по приоритетам (High, Medium, Low)
    tasks     map[TaskID]*taskState
    deps      map[TaskID][]TaskID      //кто зависит от задачи
    shutdown  bool
    wg        sync.WaitGroup           // для ожидания завершения воркеров
    nextID    atomic.Uint64            // генератор ID
    stats     Stats
}

// TODO: реализуй NewScheduler
func NewScheduler(workers int) *Scheduler {

	if workers <= 0{
		panic("Отрицательные рабочие негры")
	}

	s := *Scheduler{}
	s.cond = sync.NewCond(&s.mu)
	s.nextId := 0
	//Срез готовых задачи по приоритетам
	s.ready = make([][]*taskState, priorityLevels)
	//Мапа для хранения всех тасок
	s.tasks = make(map[TaskID]*taskState)

	for i := 0; i <= worker - 1; i++{

		s.wg.Add(1)
		go func() {

			defer s.wg.Done()
			s.worker()

		}()

	}
	return s
}

func (s *Scheduler) worker() {
    defer s.wg.Done() // при запуске в NewScheduler добавляем wg.Add(1)

    for {
        
        s.mu.Lock()

        
        if s.shutdown {
            s.mu.Unlock()
            return
        }

       
        var task *taskState
        var prio int
        found := false
        for prio = 0; prio < len(s.ready); prio++ {
            if len(s.ready[prio]) > 0 {
                task = s.ready[prio][0]
                s.ready[prio] = s.ready[prio][1:]
                found = true
                break
            }
        }

        if !found {
            
            s.cond.Wait()
            s.mu.Unlock()
            continue
        }

        
        s.mu.Unlock()

        
        if task.Ctx.Err() != nil {
           
            s.mu.Lock()
            task.Status = StatusCanceled
            task.FinishedAt = time.Now()
            task.Err = task.Ctx.Err()
           
            s.stats.Canceled.Add(1)
            s.stats.TotalExec.Add(time.Since(task.StartedAt).Nanoseconds())
            
            for _, depID := range task.Dependents {
                if dep, ok := s.tasks[depID]; ok {
                    if dep.Status == StatusPending || dep.Status == StatusReady {
                        dep.Cancel()
                        dep.Status = StatusCanceled
                        dep.FinishedAt = time.Now()
                        dep.Err = ErrDependencyFailed
                        
                        if dep.WaitCh != nil {
                            close(dep.WaitCh)
                            dep.WaitCh = nil
                        }
                        
                        s.stats.Canceled.Add(1)
                        s.stats.TotalExec.Add(time.Since(dep.StartedAt).Nanoseconds())
                    }
                }
            }
            s.mu.Unlock()
            continue
        }

        
        task.Status = StatusRunning
        task.StartedAt = time.Now()
        err := task.Fn(task.Ctx)

        
        s.mu.Lock()
        task.FinishedAt = time.Now()
        if err != nil {
            task.Status = StatusFailed
            task.Err = err
            s.stats.Failed.Add(1)
            
            for _, depID := range task.Dependents {
                if dep, ok := s.tasks[depID]; ok {
                    if dep.Status == StatusPending || dep.Status == StatusReady {
                        dep.Cancel()
                        dep.Status = StatusCanceled
                        dep.FinishedAt = time.Now()
                        dep.Err = ErrDependencyFailed
                        if dep.WaitCh != nil {
                            close(dep.WaitCh)
                            dep.WaitCh = nil
                        }
                        s.stats.Canceled.Add(1)
                        s.stats.TotalExec.Add(time.Since(dep.StartedAt).Nanoseconds())
                    }
                }
            }
        } else {
            task.Status = StatusCompleted
            s.stats.Completed.Add(1)
            
            for _, depID := range task.Dependents {
                if dep, ok := s.tasks[depID]; ok {
                    dep.RemainingDeps--
                    if dep.RemainingDeps == 0 && dep.Status == StatusPending {
                        
                        dep.Status = StatusReady
                        
                        s.ready[dep.Priority] = append(s.ready[dep.Priority], dep)
                        s.cond.Signal()
                    }
                }
            }
        }
        s.stats.TotalExec.Add(task.FinishedAt.Sub(task.StartedAt).Nanoseconds())

        
        if task.WaitCh != nil {
            close(task.WaitCh)
            task.WaitCh = nil
        }
        s.mu.Unlock()
    }
}

// TODO: реализуй Schedule — добавляет задачу в очередь
// Если у задачи есть DependsOn — ждём завершения всех зависимостей в горутине
func (s *Scheduler) Schedule(task Task) TaskID {

}

// TODO: реализуй Cancel
func (s *Scheduler) Cancel(id TaskID) bool {

}

// Wait блокируется до завершения задачи
func (s *Scheduler) Wait(id TaskID) error {


// Shutdown останавливает планировщик
func (s *Scheduler) Shutdown() {

}

func (s *Scheduler) Stats() Stats {

}

func main() {
	sched := NewScheduler(3)

	// Задача A
	idA := sched.Schedule(Task{
		Priority: PriorityHigh,
		Fn: func(ctx context.Context) error {
			time.Sleep(100 * time.Millisecond)
			fmt.Println("задача A выполнена")
			return nil
		},
	})

	// Задача B зависит от A
	idB := sched.Schedule(Task{
		Priority:  PriorityMedium,
		DependsOn: []TaskID{idA},
		Fn: func(ctx context.Context) error {
			fmt.Println("задача B выполнена (после A)")
			return nil
		},
	})

	// Задача C с дедлайном
	sched.Schedule(Task{
		Priority: PriorityLow,
		Deadline: time.Now().Add(50 * time.Millisecond),
		Fn: func(ctx context.Context) error {
			select {
			case <-time.After(200 * time.Millisecond):
				fmt.Println("задача C выполнена")
				return nil
			case <-ctx.Done():
				fmt.Println("задача C отменена по дедлайну")
				return ctx.Err()
			}
		},
	})

	sched.Wait(idB)
	time.Sleep(100 * time.Millisecond)

	stats := sched.Stats()
	fmt.Printf("Выполнено: %d, Ошибок: %d, Отменено: %d\n",
		stats.Completed, stats.Failed, stats.Cancelled)
}
