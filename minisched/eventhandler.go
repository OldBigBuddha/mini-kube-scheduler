package minisched

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
)

func addAllEventHandlers(
	sched *Scheduler,
	informerFactory informers.SharedInformerFactory,
) {
}

// assignedPod selects pods that are assigned (scheduled and running).
func assignedPod(pod *v1.Pod) bool {
	return false
}

func (sched *Scheduler) addPodToSchedulingQueue(obj interface{}) {
}
