package minisched

import (
	"context"

	"k8s.io/kubernetes/pkg/scheduler/framework"

	v1 "k8s.io/api/core/v1"

	"github.com/sanposhiho/mini-kube-scheduler/minisched/queue"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
)

type Scheduler struct {
	SchedulingQueue *queue.SchedulingQueue

	client clientset.Interface
}

func New(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
) *Scheduler {
	sched := &Scheduler{
		SchedulingQueue: queue.New(),
		client:          client,
	}

	return sched
}

func (sched *Scheduler) Run(ctx context.Context) {
}

func (sched *Scheduler) scheduleOne(ctx context.Context) {
}

func (sched *Scheduler) Bind(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) error {
	return nil
}
