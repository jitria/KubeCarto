// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package exporter

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"

	"kubecarto/protobuf"
)

// apiLogStreamInform structure
type apiLogStreamInform struct {
	Hostname  string
	IPAddress string
	stream    protobuf.KubeCarto_GetAPILogServer
}

// InsertAPILog Function
func InsertAPILog(apiLog *protobuf.APILog) {
	ExpH.exporterAPILogs <- apiLog

	// optional: label debug
	var labelString []string
	for k, v := range apiLog.SrcLabel {
		labelString = append(labelString, fmt.Sprintf("%s:%s", k, v))
	}
	sort.Strings(labelString)
}

// exportAPILogs Function
func (exp *ExpHandler) exportAPILogs(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case apiLog, ok := <-exp.exporterAPILogs:
			if !ok {
				log.Printf("[Exporter] APILogs channel closed unexpectedly")
				return
			}
			if err := exp.SendAPILogs(apiLog); err != nil {
				log.Printf("[Exporter] Failed to export API Logs: %v", err)
			}

		case <-exp.stopChan:
			return
		}
	}
}

// SendAPILogs Function
func (exp *ExpHandler) SendAPILogs(apiLog *protobuf.APILog) error {
	exp.exporterLock.Lock()
	defer exp.exporterLock.Unlock()

	failed := 0
	total := len(exp.apiLogExporters)
	newList := make([]*apiLogStreamInform, 0, total)

	for _, exporter := range exp.apiLogExporters {
		if err := exporter.stream.Send(apiLog); err != nil {
			failed++
			log.Printf("[Exporter] Failed to export an API log to %s (%s): %v",
				exporter.Hostname, exporter.IPAddress, err)
		} else {
			newList = append(newList, exporter)
		}
	}

	exp.apiLogExporters = newList

	if failed != 0 {
		msg := fmt.Sprintf("[Exporter] Failed to export API logs properly (%d/%d failed)", failed, total)
		return errors.New(msg)
	}
	return nil
}

// GetAPILog Function (for gRPC)
func (exs *ExpService) GetAPILog(info *protobuf.ClientInfo, stream protobuf.KubeCarto_GetAPILogServer) error {
	log.Printf("[Exporter] Client %s (%s) connected (GetAPILog)", info.HostName, info.IPAddress)

	currExporter := &apiLogStreamInform{
		Hostname:  info.HostName,
		IPAddress: info.IPAddress,
		stream:    stream,
	}

	ExpH.exporterLock.Lock()
	ExpH.apiLogExporters = append(ExpH.apiLogExporters, currExporter)
	ExpH.exporterLock.Unlock()

	select {}
}
