// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package collector

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	"kubecarto/manager/exporter"
	"kubecarto/protobuf"
)

// Deploy Function
func (cs *ColService) AddDeployEvent(ctx context.Context, dep *protobuf.Deploy) (*protobuf.Response, error) {
	log.Printf("[Manager] AddDeployEvent: got Deploy %s/%s cluster=%s", dep.Namespace, dep.Name, dep.Cluster)

	exporter.InsertDeployAdd(dep)
	return &protobuf.Response{Msg: 0}, nil
}

func (cs *ColService) UpdateDeployEvent(ctx context.Context, dep *protobuf.Deploy) (*protobuf.Response, error) {
	log.Printf("[Manager] UpdateDeployEvent: got Deploy %s/%s cluster=%s", dep.Namespace, dep.Name, dep.Cluster)

	exporter.InsertDeployUpdate(dep)
	return &protobuf.Response{Msg: 0}, nil
}

func (cs *ColService) DeleteDeployEvent(ctx context.Context, dep *protobuf.Deploy) (*protobuf.Response, error) {
	log.Printf("[Manager] DeleteDeployEvent: got Deploy %s/%s cluster=%s", dep.Namespace, dep.Name, dep.Cluster)

	exporter.InsertDeployDelete(dep)
	return &protobuf.Response{Msg: 0}, nil
}

// Pod Function
func (cs *ColService) AddPodEvent(ctx context.Context, pod *protobuf.Pod) (*protobuf.Response, error) {
	log.Printf("[Manager] AddPodEvent: got Pod %s/%s cluster=%s IP=%s",
		pod.Namespace, pod.Name, pod.Cluster, pod.PodIP)

	exporter.InsertPodAdd(pod)
	return &protobuf.Response{Msg: 0}, nil
}

func (cs *ColService) UpdatePodEvent(ctx context.Context, pod *protobuf.Pod) (*protobuf.Response, error) {
	log.Printf("[Manager] UpdatePodEvent: got Pod %s/%s cluster=%s IP=%s",
		pod.Namespace, pod.Name, pod.Cluster, pod.PodIP)

	exporter.InsertPodUpdate(pod)
	return &protobuf.Response{Msg: 0}, nil
}

func (cs *ColService) DeletePodEvent(ctx context.Context, pod *protobuf.Pod) (*protobuf.Response, error) {
	log.Printf("[Manager] DeletePodEvent: got Pod %s/%s cluster=%s",
		pod.Namespace, pod.Name, pod.Cluster)

	exporter.InsertPodDelete(pod)
	return &protobuf.Response{Msg: 0}, nil
}

// Service Function
func (cs *ColService) AddSvcEvent(ctx context.Context, svc *protobuf.Service) (*protobuf.Response, error) {
	log.Printf("[Manager] AddSvcEvent: got Service %s/%s cluster=%s clusterIP=%s",
		svc.Namespace, svc.Name, svc.Cluster, svc.ClusterIP)

	key := fmt.Sprintf("%s/%s/%s", svc.Cluster, svc.Namespace, svc.Name)

	if oldSvc, found := ColH.svcCache[key]; found {
		for _, oldIP := range oldSvc.ExternalIPs {
			delete(ColH.ipToService, oldIP)
		}
		for _, oldIP := range oldSvc.LoadBalancerIPs {
			delete(ColH.ipToService, oldIP)
		}
	}

	for _, extIP := range svc.ExternalIPs {
		ColH.ipToService[extIP] = &ServiceInfo{
			Cluster:   svc.Cluster,
			Namespace: svc.Namespace,
			Name:      svc.Name,
		}
	}
	for _, lbIP := range svc.LoadBalancerIPs {
		ColH.ipToService[lbIP] = &ServiceInfo{
			Cluster:   svc.Cluster,
			Namespace: svc.Namespace,
			Name:      svc.Name,
		}
	}

	ColH.svcCache[key] = svc

	exporter.InsertSvcAdd(svc)
	return &protobuf.Response{Msg: 0}, nil
}

func (cs *ColService) UpdateSvcEvent(ctx context.Context, svc *protobuf.Service) (*protobuf.Response, error) {
	log.Printf("[Manager] UpdateSvcEvent: got Service %s/%s cluster=%s clusterIP=%s",
		svc.Namespace, svc.Name, svc.Cluster, svc.ClusterIP)

	key := fmt.Sprintf("%s/%s/%s", svc.Cluster, svc.Namespace, svc.Name)

	if oldSvc, found := ColH.svcCache[key]; found {
		for _, oldIP := range oldSvc.ExternalIPs {
			delete(ColH.ipToService, oldIP)
		}
		for _, oldIP := range oldSvc.LoadBalancerIPs {
			delete(ColH.ipToService, oldIP)
		}
	}

	for _, extIP := range svc.ExternalIPs {
		ColH.ipToService[extIP] = &ServiceInfo{
			Cluster:   svc.Cluster,
			Namespace: svc.Namespace,
			Name:      svc.Name,
		}
	}
	for _, lbIP := range svc.LoadBalancerIPs {
		ColH.ipToService[lbIP] = &ServiceInfo{
			Cluster:   svc.Cluster,
			Namespace: svc.Namespace,
			Name:      svc.Name,
		}
	}
	ColH.svcCache[key] = svc

	exporter.InsertSvcUpdate(svc)
	return &protobuf.Response{Msg: 0}, nil
}

func (cs *ColService) DeleteSvcEvent(ctx context.Context, svc *protobuf.Service) (*protobuf.Response, error) {
	log.Printf("[Manager] DeleteSvcEvent: got Service %s/%s cluster=%s",
		svc.Namespace, svc.Name, svc.Cluster)

	key := fmt.Sprintf("%s/%s/%s", svc.Cluster, svc.Namespace, svc.Name)

	if oldSvc, found := ColH.svcCache[key]; found {
		for _, oldIP := range oldSvc.ExternalIPs {
			delete(ColH.ipToService, oldIP)
		}
		for _, oldIP := range oldSvc.LoadBalancerIPs {
			delete(ColH.ipToService, oldIP)
		}
		delete(ColH.svcCache, key)
	} else {
		for _, extIP := range svc.ExternalIPs {
			delete(ColH.ipToService, extIP)
		}
		for _, lbIP := range svc.LoadBalancerIPs {
			delete(ColH.ipToService, lbIP)
		}
	}

	exporter.InsertSvcDelete(svc)
	return &protobuf.Response{Msg: 0}, nil
}

// GiveAPILog Function
func (cs *ColService) GiveAPILog(stream protobuf.KubeCarto_GiveAPILogServer) error {
	for {
		// Receive APILog from stream.
		apiLog, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&protobuf.Response{Msg: 0})
		}
		if err != nil {
			return fmt.Errorf("GiveAPILog recv error: %v", err)
		}
		ColH.apiLogChan <- apiLog
	}
}

// ProcessAPILogs Function
func ProcessAPILogs(wg *sync.WaitGroup) {
	wg.Add(1)

	for {
		select {
		case logType, ok := <-ColH.apiLogChan:
			if !ok {
				log.Print("[LogProcessor] Failed to process an API log")
				continue
			}

			apiLog := logType.(*protobuf.APILog)

			if apiLog.DstCluster == "Unknown" {
				if si, found := ColH.ipToService[apiLog.DstIP]; found {
					apiLog.DstCluster = si.Cluster
					apiLog.DstNamespace = si.Namespace
					apiLog.DstName = si.Name
					apiLog.DstType = "Service"
				}
			}
			if apiLog.SrcCluster == "Unknown" {
				if si, found := ColH.ipToService[apiLog.SrcIP]; found {
					apiLog.SrcCluster = si.Cluster
					apiLog.SrcNamespace = si.Namespace
					apiLog.SrcName = si.Name
					apiLog.SrcType = "Service"
				}
			}

			go exporter.InsertAPILog(apiLog)
		case <-ColH.stopChan:
			wg.Done()
			return
		}
	}
}

// GiveEnvoyMetrics Function
func (cs *ColService) GiveEnvoyMetrics(stream protobuf.KubeCarto_GiveEnvoyMetricsServer) error {
	for {
		// Receive EnvoyMetrics from stream.
		envoyMetrics, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&protobuf.Response{Msg: 0})
		}
		if err != nil {
			return fmt.Errorf("GiveEnvoyMetrics recv error: %v", err)
		}
		ColH.metricsChan <- envoyMetrics
	}
}

// ProcessEnvoyMetrics Function
func ProcessEnvoyMetrics(wg *sync.WaitGroup) {
	wg.Add(1)

	for {
		select {
		case logType, ok := <-ColH.metricsChan:
			if !ok {
				log.Print("[LogProcessor] Failed to process Envoy metrics")
				continue
			}

			go exporter.InsertEnvoyMetrics(logType.(*protobuf.EnvoyMetrics))

		case <-ColH.stopChan:
			wg.Done()
			return
		}
	}
}
