// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package client

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	pb "kubecarto/protobuf"
)

// Feeder Structure
type Feeder struct {
	Running bool

	client            pb.KubeCartoClient
	logStream         pb.KubeCarto_GetAPILogClient
	envoyMetricStream pb.KubeCarto_GetEnvoyMetricsClient

	Done chan struct{}
}

// StrToFile Function
func StrToFile(str, targetFile string) {
	_, err := os.Stat(targetFile)
	if err != nil {
		newFile, err := os.Create(filepath.Clean(targetFile))
		if err != nil {
			fmt.Printf("[Client] Failed to create a file (%s, %s)\n", targetFile, err.Error())
			return
		}
		err = newFile.Close()
		if err != nil {
			fmt.Printf("[Client] Failed to close the file (%s, %s)\n", targetFile, err.Error())
		}
	}

	file, err := os.OpenFile(targetFile, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		fmt.Printf("[Client] Failed to open a file (%s, %s)\n", targetFile, err.Error())
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Printf("[Client] Failed to close the file (%s, %s)\n", targetFile, err.Error())
		}
	}()

	_, err = file.WriteString(str)
	if err != nil {
		fmt.Printf("[Client] Failed to write a string into the file (%s, %s)\n", targetFile, err.Error())
	}
}

// NewClient Function
func NewClient(client pb.KubeCartoClient, clientInfo *pb.ClientInfo, logCfg string, metricCfg string, metricFilter string) *Feeder {
	fd := &Feeder{}

	fd.Running = true

	fd.client = client

	fd.Done = make(chan struct{})

	if logCfg != "none" {
		// Contact the server and print out its response
		logStream, err := client.GetAPILog(context.Background(), clientInfo)
		if err != nil {
			log.Fatalf("[Client] Could not get API log: %v", err)
		}

		fd.logStream = logStream
	}

	if metricCfg != "none" && (metricFilter == "all" || metricFilter == "envoy") {
		emStream, err := client.GetEnvoyMetrics(context.Background(), clientInfo)
		if err != nil {
			log.Fatalf("[Client] Could not get Envoy metrics: %v", err)
		}

		fd.envoyMetricStream = emStream
	}

	return fd
}

// APILogRoutine Function
func (fd *Feeder) APILogRoutine(logCfg string) {
	for fd.Running {
		select {
		default:
			data, err := fd.logStream.Recv()
			if err != nil {
				log.Fatalf("[Client] Failed to receive an API log: %v", err)
				break
			}

			str := ""
			str = str + "== API Log ==\n"
			str = str + fmt.Sprintf("%v\n", data)

			if logCfg == "stdout" {
				fmt.Printf("%s", str)
			} else {
				StrToFile(str, logCfg)
			}
		case <-fd.Done:
			return
		}
	}
}

// EnvoyMetricsRoutine Function
func (fd *Feeder) EnvoyMetricsRoutine(metricCfg string) {
	metricKeys := []string{"GAUGE", "COUNTER", "HISTOGRAM", "SUMMARY"}
	for fd.Running {
		select {
		default:
			data, err := fd.envoyMetricStream.Recv()
			if err != nil {
				log.Fatalf("[Client] Failed to receive Envoy metrics: %v", err)
				break
			}

			str := ""
			str = fmt.Sprintf("== Envoy Metrics / %s ==\n", data.TimeStamp)
			str = str + fmt.Sprintf("Namespace: %s\n", data.Namespace)
			str = str + fmt.Sprintf("Name: %s\n", data.Name)
			str = str + fmt.Sprintf("IPAddress: %s\n", data.IPAddress)
			str = str + fmt.Sprintf("Labels: %s\n", data.Labels)

			for _, key := range metricKeys {
				str = str + fmt.Sprintf("%s: {%v}\n", key, data.Metrics[key])
			}

			if metricCfg == "stdout" {
				fmt.Printf("%s", str)
			} else {
				StrToFile(str, metricCfg)
			}
		case <-fd.Done:
			return
		}
	}
}
