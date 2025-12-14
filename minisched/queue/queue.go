package queue

import (
	"sync"

	v1 "k8s.io/api/core/v1"
)

type SchedulingQueue struct {
	lock    *sync.Cond
	activeQ []*v1.Pod
}

func New() *SchedulingQueue {
	return &SchedulingQueue{
		lock:    sync.NewCond(&sync.Mutex{}),
		activeQ: []*v1.Pod{},
	}
}

func (s *SchedulingQueue) Add(pod *v1.Pod) {
	s.lock.L.Lock()
	defer s.lock.L.Unlock()

	s.activeQ = append(s.activeQ, pod)
	s.lock.Signal()
}

func (s *SchedulingQueue) NextPod() *v1.Pod {
	s.lock.L.Lock()
	for len(s.activeQ) == 0 {
		s.lock.Wait()
	}

	p := s.activeQ[0]
	s.activeQ = s.activeQ[1:]
	s.lock.L.Unlock()
	return p
}
