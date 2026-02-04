// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package uploader

import (
	"fmt"
	"log"
	"sync"

	"kubecarto/agent/config"
	"kubecarto/types"

	"kubecarto/protobuf"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// UplH global reference for Uploader Handler
var UplH *UplHandler

// init Function
func init() {
	UplH = NewUploaderHandler()
}

// UplHandler Structure
type UplHandler struct {
	grpcClient protobuf.KubeCartoClient

	uploaderAPILogs      chan *protobuf.APILog
	uploaderEnvoyMetrics chan *protobuf.EnvoyMetrics
	clusterEvents        chan *types.ClusterEvent

	stopChan chan struct{}
}

// NewUploaderHandler Function
func NewUploaderHandler() *UplHandler {
	ch := &UplHandler{
		uploaderAPILogs:      make(chan *protobuf.APILog),
		uploaderEnvoyMetrics: make(chan *protobuf.EnvoyMetrics),
		clusterEvents:        make(chan *types.ClusterEvent),

		stopChan: make(chan struct{}),
	}
	return ch
}

// StartUploader Function
func StartUploader(wg *sync.WaitGroup) bool {
	grpcClient, err := connectToManager()
	if err != nil {
		log.Printf("[Uploader] Failed to connect to Manager's gRPC server")
		return false
	}
	UplH.grpcClient = grpcClient

	// Export ClusterEvent
	go UplH.uploadClusterEvent(wg)
	log.Printf("[Uploader] Exporting Cluster information through gRPC services")

	// Export APILogs
	go UplH.uploadAPILogs(wg)
	log.Printf("[Uploader] Exporting API logs through gRPC services")

	// Export EnvoyMetrics
	go UplH.uploadEnvoyMetrics(wg)
	log.Printf("[Uploader] Exporting Envoy metrics through gRPC services")

	return true
}

func connectToManager() (protobuf.KubeCartoClient, error) {
	managerAddr := fmt.Sprintf("%s:%s", config.GlobalConfig.ManagerAddr, config.GlobalConfig.ManagerPort)

	conn, err := grpc.NewClient(managerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[Uploader] Failed to connect to Manager's gRPC server at %s: %v", managerAddr, err)
		return nil, err
	}

	client := protobuf.NewKubeCartoClient(conn)

	return client, nil
}

// StopUploader Function
func StopUploader() bool {
	// One for uploadClusterInfo
	UplH.stopChan <- struct{}{}

	// One for uploadAPILogs
	UplH.stopChan <- struct{}{}

	// One for uploadEnvoyMetrics
	UplH.stopChan <- struct{}{}

	return true
}
