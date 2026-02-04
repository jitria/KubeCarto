// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package collector

import (
	"fmt"
	"log"
	"net"
	"sync"

	"kubecarto/manager/config"
	"kubecarto/protobuf"

	"google.golang.org/grpc"
)

type ColService struct {
	protobuf.UnimplementedKubeCartoServer
}

// ColH global reference for Collector Handler
var ColH *ColHandler

type ServiceInfo struct {
	Cluster   string
	Namespace string
	Name      string
}

// ColHandler Structure
type ColHandler struct {
	colService  net.Listener
	grpcServer  *grpc.Server
	grpcService *ColService

	ipToService map[string]*ServiceInfo      // key: IP, value: serviceInfo
	svcCache    map[string]*protobuf.Service // key: "cluster/namespace/name" value: *protobuf.Service

	apiLogChan  chan interface{}
	metricsChan chan interface{}

	stopChan chan struct{}
}

// init Function
func init() {
	ColH = NewColHandler()
}

// NewColHandler Function
func NewColHandler() *ColHandler {
	lh := &ColHandler{
		grpcService: new(ColService),
		ipToService: make(map[string]*ServiceInfo),
		svcCache:    make(map[string]*protobuf.Service),

		apiLogChan:  make(chan interface{}),
		metricsChan: make(chan interface{}),

		stopChan: make(chan struct{}),
	}

	return lh
}

// StartCollector Function
func StartCollector(wg *sync.WaitGroup) bool {
	// Make a string with the given collector address and port
	collectorService := fmt.Sprintf("%s:%s", config.GlobalConfig.CollectorAddr, config.GlobalConfig.CollectorPort)

	// Start listening gRPC port
	colService, err := net.Listen("tcp", collectorService)
	if err != nil {
		log.Printf("[Collector] Failed to listen at %s: %v", collectorService, err)
		return false
	}
	ColH.colService = colService

	log.Printf("[Collector] Listening Collector gRPC services (%s)", collectorService)

	// Create gRPC Service
	gRPCServer := grpc.NewServer()
	ColH.grpcServer = gRPCServer

	protobuf.RegisterKubeCartoServer(gRPCServer, ColH.grpcService)

	// Serve gRPC Service
	go ColH.grpcServer.Serve(ColH.colService)

	log.Print("[Collector] Serving Collector gRPC services")

	// handle API logs
	go ProcessAPILogs(wg)

	// handle Envoy metrics
	go ProcessEnvoyMetrics(wg)

	log.Print("[LogProcessor] Started Log Processors")

	return true
}

// StopCollector Function
func StopCollector() bool {
	ColH.grpcServer.GracefulStop()

	log.Print("[Collector] Gracefully stopped Collector gRPC services")

	// One for ProcessAPILogs
	ColH.stopChan <- struct{}{}

	// One for ProcessMetrics
	ColH.stopChan <- struct{}{}

	log.Print("[LogProcessor] Stopped Log Processors")

	return true
}
