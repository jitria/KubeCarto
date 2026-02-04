// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package processor

import (
	"log"
	"sync"
)

// LogH global reference for Log Handler
var LogH *LogHandler

// init Function
func init() {
	LogH = NewLogHandler()
}

// LogHandler Structure
type LogHandler struct {
	stopChan chan struct{}

	apiLogChan  chan interface{}
	metricsChan chan interface{}
}

// NewLogHandler Function
func NewLogHandler() *LogHandler {
	lh := &LogHandler{
		stopChan: make(chan struct{}),

		apiLogChan:  make(chan interface{}),
		metricsChan: make(chan interface{}),
	}

	return lh
}

// StartLogProcessor Function
func StartLogProcessor(wg *sync.WaitGroup) bool {
	// handle API logs
	go processAPILogs(wg)

	// handle Envoy metrics
	go processEnvoyMetrics(wg)

	log.Print("[LogProcessor] Started Log Processors")

	return true
}

// StopLogProcessor Function
func StopLogProcessor() bool {
	// One for processAPILogs
	LogH.stopChan <- struct{}{}

	// One for processMetrics
	LogH.stopChan <- struct{}{}

	log.Print("[LogProcessor] Stopped Log Processors")

	return true
}
