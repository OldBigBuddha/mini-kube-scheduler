package minisched

import (
	"fmt"

	"github.com/OldBigBuddha/mini-kube-scheduler/minisched/plugins/score/nodenumber"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func createScorePlugins() ([]framework.ScorePlugin, error) {
	nnp, err := createNodeNumberPlugin()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize nodenumber plugin: %w", err)
	}

	scorePlugins := []framework.ScorePlugin{
		nnp.(framework.ScorePlugin),
	}

	return scorePlugins, nil
}

var nodenumberplugin framework.Plugin

func createNodeNumberPlugin() (framework.Plugin, error) {
	if nodenumberplugin != nil {
		return nodenumberplugin, nil
	}

	plugin, err := nodenumber.New(nil, nil)
	nodenumberplugin = plugin
	return plugin, err
}
