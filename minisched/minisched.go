package minisched

import (
	"context"
	"fmt"
	"math/rand"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/OldBigBuddha/mini-kube-scheduler/minisched/queue"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
)

type Scheduler struct {
	SchedulingQueue *queue.SchedulingQueue

	client clientset.Interface

	filterPlugins []framework.FilterPlugin
	scorePlugins  []framework.ScorePlugin
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

	// filter nodes by filter plugins
	feasibleNodes, err := sched.RunFilterPlugins(ctx, nil, pod, nodes.Items)
	if err != nil {
		klog.Error(err)
		return
	}

	klog.Infof("minischeduler: Found %d feasible nodes for pod %s/%s", len(feasibleNodes), pod.Namespace, pod.Name)
	klog.Infof("minischeduler: Feasible nodes are: %v", feasibleNodes)

	// score each feasible node by score plugins
	score, status := sched.RunScorePlugins(ctx, nil, pod, feasibleNodes)
	if !status.IsSuccess() {
		klog.Error(status.AsError())
		return
	}

	klog.Infof("minischeduler: Scoring results for pod %s/%s: %v", pod.Namespace, pod.Name, score)

	hostname, err := sched.selectHost(score)
	if err != nil {
		klog.Error(err)
		return
	}

	// bind pod to node
	if err := sched.Bind(ctx, pod, hostname); err != nil {
		klog.Error(err)
		return
	}
}

func (sched *Scheduler) RunFilterPlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []v1.Node) ([]*v1.Node, error) {
	feasibleNodes := make([]*v1.Node, 0, len(nodes))

	// Records which plugin rejected the excluded node
	diagnosis := framework.Diagnosis{
		NodeToStatusMap:      make(framework.NodeToStatusMap),
		UnschedulablePlugins: sets.NewString(),
	}

	// TODO: consider about nominated pod
	for _, n := range nodes {
		nodeInfo := framework.NewNodeInfo()
		nodeInfo.SetNode(&n)

		status := framework.NewStatus(framework.Success)
		for _, pl := range sched.filterPlugins {
			status := pl.Filter(ctx, state, pod, nodeInfo)
			// IsSuccess means that is the node feasible
			if !status.IsSuccess() {
				// Records which plugin rejected the node
				status.SetFailedPlugin(pl.Name())
				diagnosis.UnschedulablePlugins.Insert(status.FailedPlugin())
				break
			}
		}
		if status.IsSuccess() {
			feasibleNodes = append(feasibleNodes, nodeInfo.Node())
		}
	}

	if len(feasibleNodes) == 0 {
		return nil, &framework.FitError{
			Pod:       pod,
			Diagnosis: diagnosis,
		}
	}
	return feasibleNodes, nil
}

func (sched *Scheduler) RunScorePlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) (framework.NodeScoreList, *framework.Status) {
	scoresMap := sched.createPluginToNodeScores(nodes)

	for index, n := range nodes {
		for _, pl := range sched.scorePlugins {
			score, status := pl.Score(ctx, state, pod, n.Name)
			if status.IsSuccess() {
				// Usually, score plugin will return success
				return nil, status
			}
			scoresMap[pl.Name()][index] = framework.NodeScore{
				Name:  n.Name,
				Score: score,
			}
		}
	}

	// TODO: plugin weight & normalize scores

	// Aggregate scores from all score plugins
	result := make(framework.NodeScoreList, 0, len(nodes))
	for i := range nodes {
		result = append(result, framework.NodeScore{
			Name:  nodes[i].Name,
			Score: 0,
		})
		for j := range scoresMap {
			result[i].Score += scoresMap[j][i].Score
		}
	}

	return result, nil
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

// --- helper functions ---

func (sched *Scheduler) selectHost(nodeScoreList framework.NodeScoreList) (string, error) {
	if len(nodeScoreList) == 0 {
		return "", fmt.Errorf("empty priority list")
	}

	maxScore := nodeScoreList[0].Score
	selected := nodeScoreList[0].Name
	cntOfmaxScore := 1
	for _, ns := range nodeScoreList[1:] {
		if ns.Score > maxScore {
			maxScore = ns.Score
			selected = ns.Name
			cntOfmaxScore = 1
		} else if ns.Score == maxScore {
			cntOfmaxScore++

			// Replace selected node with probability of 1/cntOfmaxScore
			if rand.Intn(cntOfmaxScore) == 0 {
				selected = ns.Name
			}
		}
	}
	return selected, nil
}

func (sched *Scheduler) createPluginToNodeScores(nodes []*v1.Node) framework.PluginToNodeScores {
	pluginToNodeScores := make(framework.PluginToNodeScores)

	for _, pl := range sched.scorePlugins {
		pluginToNodeScores[pl.Name()] = make(framework.NodeScoreList, len(nodes))
	}

	return pluginToNodeScores
}
