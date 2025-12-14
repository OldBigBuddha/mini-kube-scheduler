package minisched

import (
	"context"
	"math/rand"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/OldBigBuddha/mini-kube-scheduler/minisched/queue"
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
	pod := sched.SchedulingQueue.NextPod()

	nodes, err := sched.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Error(err)
		return
	}
	klog.Info("minischeduler: Got Nodes successfully")

	// pick a random node
	rand.Seed(time.Now().UnixNano())
	selectedNode := nodes.Items[rand.Intn(len(nodes.Items))]

	// bind pod to node
	if err := sched.Bind(ctx, pod, selectedNode.Name); err != nil {
		klog.Error(err)
		return
	}
}

func (sched *Scheduler) Bind(ctx context.Context, p *v1.Pod, nodeName string) error {
	binding := &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: p.Namespace,
			Name:      p.Name,
			UID:       p.UID,
		},
	}

	if err := sched.client.CoreV1().Pods(binding.Namespace).Bind(ctx, binding, metav1.CreateOptions{}); err != nil {
		return err
	}

	klog.Infof("minischeduler: Pod %s/%s is bound to %s", p.Namespace, p.Name, nodeName)
	return nil
}
