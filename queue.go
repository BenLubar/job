package job

import (
	"container/heap"
	"sync"
)

func Work(jobs <-chan interface{}, options *Options) <-chan interface{} {
	if options == nil {
		options = &Options{}
	}
	options.defaults()

	input, output := make(chan *job), make(chan *job)

	var wg sync.WaitGroup

	wg.Add(options.NumWorkers)
	go func() {
		wg.Wait()
		close(output)
	}()

	jh := new(jobHeap)
	heap.Init(jh)

	limiter := make(chan struct{}, options.MaxWaiting)
	go jobCreator(jobs, input, limiter)

	for i := 0; i < options.NumWorkers; i++ {
		go worker(input, output, &wg, options.Work)
	}

	completed := make(chan interface{})
	go func() {
		defer close(completed)
		seq, done := 0, false
		var out *interface{}
		for !done || jh.Len() > 0 {
			var (
				ready <-chan *job
				send  chan<- interface{}
				o     interface{}
			)

			if !done {
				ready = output
			}
			if out != nil {
				send, o = completed, *out
			}

			select {
			case j, ok := <-ready:
				if ok {
					heap.Push(jh, j)
				} else {
					done = true
				}

			case send <- o:
				<-limiter
				seq++
				out = nil
			}

			if out == nil && jh.Len() > 0 && (*jh)[0].seq == seq {
				out = &heap.Pop(jh).(*job).out
			}
		}
	}()

	return completed
}

func jobCreator(jobs <-chan interface{}, input chan<- *job, limiter chan<- struct{}) {
	defer close(input)

	i := 0

	for v := range jobs {
		limiter <- struct{}{}
		input <- &job{
			seq: i,
			in:  v,
		}

		i++
	}
}

func worker(input <-chan *job, output chan<- *job, wg *sync.WaitGroup, work func(interface{}) interface{}) {
	defer wg.Done()

	for j := range input {
		j.out = work(j.in)

		output <- j
	}
}

type job struct {
	seq     int
	in, out interface{}
}

type jobHeap []*job

func (h *jobHeap) Len() int {
	return len(*h)
}

func (h *jobHeap) Less(i, j int) bool {
	return (*h)[i].seq < (*h)[j].seq
}

func (h *jobHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *jobHeap) Push(x interface{}) {
	*h = append(*h, x.(*job))
}

func (h *jobHeap) Pop() interface{} {
	l := len(*h)
	j := (*h)[l-1]
	*h = (*h)[:l-1]
	return j
}

type Options struct {
	Work       func(interface{}) interface{}
	NumWorkers int // default 1
	MaxWaiting int // default 10
}

func (o *Options) defaults() {
	if o.Work == nil {
		o.Work = func(v interface{}) interface{} { return v }
	}
	if o.NumWorkers <= 0 {
		o.NumWorkers = 1
	}
	if o.MaxWaiting <= 0 {
		o.MaxWaiting = 10
	}
}
