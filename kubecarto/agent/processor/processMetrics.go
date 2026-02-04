// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package processor

import (
	"log"
	"sync"

	"kubecarto/protobuf"

	"kubecarto/agent/uploader"
)

// InsertMetrics Function
func InsertMetrics(data interface{}) {
	LogH.metricsChan <- data
}

// processEnvoyMetrics Function
func processEnvoyMetrics(wg *sync.WaitGroup) {
	wg.Add(1)

	for {
		select {
		case logType, ok := <-LogH.metricsChan:
			if !ok {
				log.Print("[LogProcessor] Failed to process Envoy metrics")
			}

			go uploader.UploadEnvoyMetrics(logType.(*protobuf.EnvoyMetrics))

		case <-LogH.stopChan:
			wg.Done()
			return
		}
	}
}
