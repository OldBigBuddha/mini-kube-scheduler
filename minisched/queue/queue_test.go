package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
)

func TestSchedulingQueueOperation(t *testing.T) {
	t.Run("TestAdd", func(t *testing.T) {
		queue := New()
		queue.Add(&v1.Pod{})
		require.Len(t, queue.activeQ, 1, "Pod should be added to the queue")

		queue.Add(&v1.Pod{})
		queue.Add(&v1.Pod{})
		require.Len(t, queue.activeQ, 3, "Three Pods should be in the queue")
	})

	t.Run("TestNextPod_dequeue", func(t *testing.T) {
		queue := New()
		queue.Add(&v1.Pod{Spec: v1.PodSpec{NodeName: "node1"}})
		queue.Add(&v1.Pod{Spec: v1.PodSpec{NodeName: "node2"}})
		require.Len(t, queue.activeQ, 2, "Two Pods should be in the queue")
		require.Equal(t, "node1", queue.NextPod().Spec.NodeName, "NextPod should return the first added pod")
		require.Len(t, queue.activeQ, 1, "One Pod should remain in the queue after NextPod")
		require.Equal(t, "node2", queue.NextPod().Spec.NodeName, "NextPod should return the second added pod")
		require.Len(t, queue.activeQ, 0, "No Pods should remain in the queue after NextPod")
	})

	t.Run("TestNextPod_blocking", func(t *testing.T) {
		queue := New()
		done := make(chan struct{})

		go func() {
			pod := queue.NextPod()
			require.Equal(t, "node-blocking", pod.Spec.NodeName, "NextPod should return the added pod after blocking")
			close(done)
		}()

		time.Sleep(100 * time.Millisecond)

		select {
		case <-done:
			t.Fatal("NextPod should have blocked, but it returned early")
		case <-time.After(50 * time.Millisecond):
			// Expected path: NextPod is still blocking
		}

		// Ensure the goroutine is likely waiting on NextPod
		// before we add the pod.
		queue.Add(&v1.Pod{Spec: v1.PodSpec{NodeName: "node-blocking"}})

		select {
		case <-done:
			// Expected path: NextPod has returned after adding the pod
		case <-time.After(100 * time.Millisecond):
			t.Fatal("NextPod did not return after adding a pod")
		}
	})
}
