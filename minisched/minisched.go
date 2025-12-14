package minisched

import (
	"context"
	"math/rand"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

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

	addAllEventHandlers(sched, informerFactory)

	return sched
}

func (sched *Scheduler) Run(ctx context.Context) {
	wait.UntilWithContext(ctx, sched.scheduleOne, 0)
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
	klog.Infof("minischeduler: Selected node name: %s", selectedNode.Name)

	// bind pod to node
	if err := sched.Bind(ctx, pod, selectedNode.Name); err != nil {
		klog.Error(err)
		return
	}
}

func (sched *Scheduler) Bind(ctx context.Context, p *v1.Pod, nodeName string) error {
	klog.Infof("minischeduler: Try to bind %s/%s to %s", p.Namespace, p.Name, nodeName)
	binding := &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: p.Namespace,
			Name:      p.Name,
			UID:       p.UID,
		},
		Target: v1.ObjectReference{
			Kind: "Node",
			Name: nodeName,
		},
	}

	if err := sched.client.CoreV1().
		Pods(binding.Namespace).
		Bind(ctx, binding, metav1.CreateOptions{}); err != nil {
		return err
	}

	klog.Infof("minischeduler: Pod %s/%s is bound to %s", p.Namespace, p.Name, nodeName)
	return nil
}
