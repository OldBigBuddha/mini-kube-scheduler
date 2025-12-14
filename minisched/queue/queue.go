package queue

import (
	"sync"

	v1 "k8s.io/api/core/v1"
)

type SchedulingQueue struct {
	activeQ []*v1.Pod
	lock    sync.RWMutex
}

func New() *SchedulingQueue {
	return &SchedulingQueue{
		activeQ: []*v1.Pod{},
	}
}

func (s *SchedulingQueue) Add(pod *v1.Pod) error {
	return nil
}

func (s *SchedulingQueue) NextPod() *v1.Pod {
	return nil
}
