// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package core

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"kubecarto/agent/collector"
	"kubecarto/agent/config"
	"kubecarto/agent/k8s"
	"kubecarto/agent/processor"
	"kubecarto/agent/uploader"
)

// StopChan Channel
var StopChan chan struct{}

// init Function
func init() {
	StopChan = make(chan struct{})
}

// AgentService Structure
type AgentService struct {
	waitGroup *sync.WaitGroup
}

// NewAgent Function
func NewAgent() *AgentService {
	sf := new(AgentService)
	sf.waitGroup = new(sync.WaitGroup)
	return sf
}

// DestroyAgent Function
func (sf *AgentService) DestroyAgent() {
	close(StopChan)

	// Remove Agent collector config from Kubernetes
	if k8s.UnpatchIstioConfigMap() {
		log.Print("[Agent] Unpatched Istio ConfigMap")
	} else {
		log.Print("[Agent] Failed to unpatch Istio ConfigMap")
	}

	// Stop collector
	if collector.StopCollector() {
		log.Print("[Agent] Stopped Collectors")
	} else {
		log.Print("[Agent] Failed to stop Collectors")
	}

	// Stop Log Processor
	if processor.StopLogProcessor() {
		log.Print("[Agent] Stopped Log Processors")
	} else {
		log.Print("[Agent] Failed to stop Log Processors")
	}

	// Stop uploader
	if uploader.StopUploader() {
		log.Print("[Agent] Stopped Uploader")
	} else {
		log.Print("[Agent] Failed to stop Uploader")
	}

	log.Print("[Agent] Waiting for routine terminations")

	sf.waitGroup.Wait()

	log.Print("[Agent] Terminated Agent")
}

// GetOSSigChannel Function
func GetOSSigChannel() chan os.Signal {
	c := make(chan os.Signal, 1)

	signal.Notify(c,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		os.Interrupt)

	return c
}

// Agent Function
func Agent() {
	sf := NewAgent()

	log.Print("[Agent] Initializing Agent")

	// Start collector
	if !collector.StartCollector() {
		sf.DestroyAgent()
		return
	}

	// Start log processor
	if !processor.StartLogProcessor(sf.waitGroup) {
		sf.DestroyAgent()
		return
	}

	// Start exporter
	if !uploader.StartUploader(sf.waitGroup) {
		sf.DestroyAgent()
		return
	}

	// Initialize Kubernetes client
	if !k8s.InitK8sClient() {
		sf.DestroyAgent()
		return
	}

	// Start Kubernetes informers
	k8s.RunInformers(StopChan, sf.waitGroup)

	// Patch Istio ConfigMap
	if !k8s.PatchIstioConfigMap() {
		sf.DestroyAgent()
		return
	}

	// Patch Namespaces
	if config.GlobalConfig.PatchingNamespaces {
		if !k8s.PatchNamespaces() {
			sf.DestroyAgent()
			return
		}
	}

	// Patch Deployments
	if config.GlobalConfig.RestartingPatchedDeployments {
		if !k8s.RestartDeployments() {
			sf.DestroyAgent()
			return
		}
	}

	log.Print("[Agent] Initialization is completed")

	// listen for interrupt signals
	sigChan := GetOSSigChannel()
	<-sigChan
	log.Print("Got a signal to terminate Agent")

	// Destroy Agent
	sf.DestroyAgent()
}
