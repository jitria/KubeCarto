// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package exporter

import (
	"fmt"
	"log"
	"net"
	"sync"

	"kubecarto/manager/config"
	"kubecarto/protobuf"

	"google.golang.org/grpc"
)

// ExpH global reference for Exporter Handler
var ExpH *ExpHandler

// init Function
func init() {
	ExpH = NewExporterHandler()
}

// ExpHandler structure
type ExpHandler struct {
	exporterService net.Listener
	grpcServer      *grpc.Server
	grpcService     *ExpService

	apiLogExporters       []*apiLogStreamInform
	envoyMetricsExporters []*envoyMetricsStreamInform

	deployAddExporters    []*deployAddStreamInform
	deployUpdateExporters []*deployUpdateStreamInform
	deployDeleteExporters []*deployDeleteStreamInform

	podAddExporters    []*podAddStreamInform
	podUpdateExporters []*podUpdateStreamInform
	podDeleteExporters []*podDeleteStreamInform

	svcAddExporters    []*svcAddStreamInform
	svcUpdateExporters []*svcUpdateStreamInform
	svcDeleteExporters []*svcDeleteStreamInform

	exporterLock sync.Mutex

	exporterAPILogs chan *protobuf.APILog
	exporterMetrics chan *protobuf.EnvoyMetrics

	exporterDeployAdd    chan *protobuf.Deploy
	exporterDeployUpdate chan *protobuf.Deploy
	exporterDeployDelete chan *protobuf.Deploy

	exporterPodAdd    chan *protobuf.Pod
	exporterPodUpdate chan *protobuf.Pod
	exporterPodDelete chan *protobuf.Pod

	exporterSvcAdd    chan *protobuf.Service
	exporterSvcUpdate chan *protobuf.Service
	exporterSvcDelete chan *protobuf.Service

	stopChan chan struct{}
}

// ExpService Structure
type ExpService struct {
	protobuf.UnimplementedKubeCartoServer
}

// NewExporterHandler Function
func NewExporterHandler() *ExpHandler {
	exp := &ExpHandler{
		grpcService: new(ExpService),

		apiLogExporters:       make([]*apiLogStreamInform, 0),
		envoyMetricsExporters: make([]*envoyMetricsStreamInform, 0),

		deployAddExporters:    make([]*deployAddStreamInform, 0),
		deployUpdateExporters: make([]*deployUpdateStreamInform, 0),
		deployDeleteExporters: make([]*deployDeleteStreamInform, 0),

		podAddExporters:    make([]*podAddStreamInform, 0),
		podUpdateExporters: make([]*podUpdateStreamInform, 0),
		podDeleteExporters: make([]*podDeleteStreamInform, 0),

		svcAddExporters:    make([]*svcAddStreamInform, 0),
		svcUpdateExporters: make([]*svcUpdateStreamInform, 0),
		svcDeleteExporters: make([]*svcDeleteStreamInform, 0),

		exporterLock: sync.Mutex{},

		exporterAPILogs: make(chan *protobuf.APILog),
		exporterMetrics: make(chan *protobuf.EnvoyMetrics),

		exporterDeployAdd:    make(chan *protobuf.Deploy),
		exporterDeployUpdate: make(chan *protobuf.Deploy),
		exporterDeployDelete: make(chan *protobuf.Deploy),

		exporterPodAdd:    make(chan *protobuf.Pod),
		exporterPodUpdate: make(chan *protobuf.Pod),
		exporterPodDelete: make(chan *protobuf.Pod),

		exporterSvcAdd:    make(chan *protobuf.Service),
		exporterSvcUpdate: make(chan *protobuf.Service),
		exporterSvcDelete: make(chan *protobuf.Service),

		stopChan: make(chan struct{}),
	}

	return exp
}

// StartExporter Function
func StartExporter(wg *sync.WaitGroup) bool {
	// Make a string with the given exporter address and port
	exporterService := fmt.Sprintf("%s:%s", config.GlobalConfig.ExporterAddr, config.GlobalConfig.ExporterPort)

	// Start listening gRPC port
	expService, err := net.Listen("tcp", exporterService)
	if err != nil {
		log.Printf("[Exporter] Failed to listen at %s: %v", exporterService, err)
		return false
	}
	ExpH.exporterService = expService

	log.Printf("[Exporter] Listening Exporter gRPC services (%s)", exporterService)

	// Create gRPC server
	gRPCServer := grpc.NewServer()
	ExpH.grpcServer = gRPCServer

	protobuf.RegisterKubeCartoServer(gRPCServer, ExpH.grpcService)

	log.Printf("[Exporter] Initialized Exporter gRPC services")

	// Serve gRPC Service
	go ExpH.grpcServer.Serve(ExpH.exporterService)

	log.Printf("[Exporter] Serving Exporter gRPC services (%s)", exporterService)

	// Export ClusterInfo
	go ExpH.exportClusterHandler(wg)

	log.Printf("[Exporter] Exporting Cluster information through gRPC services")

	// Export APILogs
	go ExpH.exportAPILogs(wg)

	log.Printf("[Exporter] Exporting API logs through gRPC services")

	// Export EnvoyMetrics
	go ExpH.exportEnvoyMetrics(wg)

	log.Printf("[Exporter] Exporting Envoy metrics through gRPC services")

	return true
}

// StopExporter Function
func StopExporter() bool {
	// One for exportClusterHandler
	ExpH.stopChan <- struct{}{}

	// One for exportAPILogs
	ExpH.stopChan <- struct{}{}

	// One for exportEnvoyMetrics
	ExpH.stopChan <- struct{}{}

	// Stop gRPC server
	ExpH.grpcServer.GracefulStop()

	log.Printf("[Exporter] Gracefully stopped Exporter gRPC services")

	return true
}
