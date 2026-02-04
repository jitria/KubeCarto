// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package core

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"kubecarto/manager/collector"
	"kubecarto/manager/exporter"
)

// StopChan Channel
var StopChan chan struct{}

// init Function
func init() {
	StopChan = make(chan struct{})
}

// ManagerService Structure
type ManagerService struct {
	waitGroup *sync.WaitGroup
}

// NewManager Function
func NewManager() *ManagerService {
	sfo := new(ManagerService)
	sfo.waitGroup = new(sync.WaitGroup)
	return sfo
}

// DestroyManager Function
func (sfo *ManagerService) DestroyManager() {
	close(StopChan)

	// Stop collector
	if collector.StopCollector() {
		log.Print("[Manager] Stopped Collectors")
	} else {
		log.Print("[Manager] Failed to stop Collectors")
	}

	// Stop exporter
	if exporter.StopExporter() {
		log.Print("[Manager] Stopped Exporters")
	} else {
		log.Print("[Manager] Failed to stop Exporters")
	}

	log.Print("[Manager] Waiting for routine terminations")

	sfo.waitGroup.Wait()

	log.Print("[Manager] Terminated Manager")
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

// Manager Function
func Manager() {
	sfo := NewManager()

	log.Print("[Manager] Initializing Manager")

	// Start collector
	if !collector.StartCollector(sfo.waitGroup) {
		sfo.DestroyManager()
		return
	}

	// Start exporter
	if !exporter.StartExporter(sfo.waitGroup) {
		sfo.DestroyManager()
		return
	}

	log.Print("[Manager] Initialization is completed")

	// listen for interrupt signals
	sigChan := GetOSSigChannel()
	<-sigChan
	log.Print("Got a signal to terminate Manager")

	// Destroy Manager
	sfo.DestroyManager()
}
