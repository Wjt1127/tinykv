package worker

import "sync"

type TaskStop struct{}

type Task interface{}

type Worker struct {
	name     string
	sender   chan<- Task
	receiver <-chan Task
	closeCh  chan struct{}
	wg       *sync.WaitGroup
}

type TaskHandler interface {
	Handle(t Task)
}

type Starter interface {
	Start()
}

func (w *Worker) Start(handler TaskHandler) {
	w.wg.Add(1) // sync.WaitGroup 经常用于等待一组 goroutine 完成执行后再继续执行其他代码。
	go func() {
		// Add(1) 与 wg.Done() 配对使用，后者用于在 goroutine 完成执行时递减计数器。
		defer w.wg.Done()
		if s, ok := handler.(Starter); ok {
			s.Start()
		}
		for { // 死循环直到接收到 TaskStop 信号
			// 阻塞态的接受
			Task := <-w.receiver
			if _, ok := Task.(TaskStop); ok {
				return
			}
			handler.Handle(Task)
		}
	}()
}

func (w *Worker) Sender() chan<- Task {
	return w.sender
}

func (w *Worker) Stop() {
	w.sender <- TaskStop{}
}

const defaultWorkerCapacity = 128

func NewWorker(name string, wg *sync.WaitGroup) *Worker {
	ch := make(chan Task, defaultWorkerCapacity)
	return &Worker{
		// sender 和 receiver 其实就是一个 channel ，但是为了外部更清晰方便地使用这个channel
		sender:   (chan<- Task)(ch),
		receiver: (<-chan Task)(ch),
		name:     name,
		wg:       wg,
	}
}
