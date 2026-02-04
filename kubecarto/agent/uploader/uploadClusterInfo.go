// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package uploader

import (
	"context"
	"log"
	"sync"
	"time"

	"kubecarto/agent/config"
	"kubecarto/types"

	"kubecarto/protobuf"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// UploadClusterEvent Function
func (upl *UplHandler) UploadClusterEvent(resourceType, action string, obj interface{}) {
	event := &types.ClusterEvent{
		ResourceType: resourceType,
		Action:       action,
		Object:       obj,
	}
	upl.clusterEvents <- event
}

// uploadClusterEvent Function
func (upl *UplHandler) uploadClusterEvent(wg *sync.WaitGroup) {
	wg.Add(1)

	for {
		select {
		case evt := <-upl.clusterEvents:
			if evt == nil {
				continue
			}
			upl.handleClusterEvent(evt)

		case <-upl.stopChan:
			wg.Done()
			return
		}
	}
}

// handleClusterEvent Function
func (upl *UplHandler) handleClusterEvent(evt *types.ClusterEvent) {
	switch evt.ResourceType {
	case "Pod":
		upl.handlePodEvent(evt.Action, evt.Object)
	case "Service":
		upl.handleServiceEvent(evt.Action, evt.Object)
	case "Deploy":
		upl.handleDeployEvent(evt.Action, evt.Object)
	default:
		log.Printf("[Uploader] Unknown resource type: %s", evt.ResourceType)
	}
}

// handlePodEvent Function
func (upl *UplHandler) handlePodEvent(action string, obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		log.Printf("[Uploader] handlePodEvent: Not a *corev1.Pod object")
		return
	}
	if upl.grpcClient == nil {
		return
	}

	podProto := convertPodToProto(pod)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch action {
	case "ADD":
		resp, err := upl.grpcClient.AddPodEvent(ctx, podProto)
		if err != nil {
			log.Printf("[Uploader] Failed to AddPodEvent for Pod %s/%s: %v",
				pod.Namespace, pod.Name, err)
			return
		}
		log.Printf("[Uploader] handlePodEvent: ADD Pod %s/%s => Manager resp=%v",
			pod.Namespace, pod.Name, resp)

	case "UPDATE":
		resp, err := upl.grpcClient.UpdatePodEvent(ctx, podProto)
		if err != nil {
			log.Printf("[Uploader] Failed to UpdatePodEvent for Pod %s/%s: %v",
				pod.Namespace, pod.Name, err)
			return
		}
		log.Printf("[Uploader] handlePodEvent: UPDATE Pod %s/%s => Manager resp=%v",
			pod.Namespace, pod.Name, resp)

	case "DELETE":
		delPodProto := &protobuf.Pod{
			Cluster:   podProto.Cluster,
			Namespace: pod.Namespace,
			Name:      pod.Name,
		}
		resp, err := upl.grpcClient.DeletePodEvent(ctx, delPodProto)
		if err != nil {
			log.Printf("[Uploader] Failed to DeletePodEvent for Pod %s/%s: %v",
				pod.Namespace, pod.Name, err)
			return
		}
		log.Printf("[Uploader] handlePodEvent: DELETE Pod %s/%s => Manager resp=%v",
			pod.Namespace, pod.Name, resp)

	default:
		log.Printf("[Uploader] handlePodEvent: Unrecognized action=%s for Pod %s/%s",
			action, pod.Namespace, pod.Name)
	}
}

// handleServiceEvent Function
func (upl *UplHandler) handleServiceEvent(action string, obj interface{}) {
	svc, ok := obj.(*corev1.Service)
	if !ok {
		log.Printf("[Uploader] handleServiceEvent: Not a *corev1.Service object")
		return
	}
	if upl.grpcClient == nil {
		return
	}

	svcProto := convertServiceToProto(svc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch action {
	case "ADD":
		resp, err := upl.grpcClient.AddSvcEvent(ctx, svcProto)
		if err != nil {
			log.Printf("[Uploader] Failed to AddSvcEvent for Service %s/%s: %v",
				svc.Namespace, svc.Name, err)
			return
		}
		log.Printf("[Uploader] handleServiceEvent: ADD Service %s/%s => Manager resp=%v",
			svc.Namespace, svc.Name, resp)

	case "UPDATE":
		resp, err := upl.grpcClient.UpdateSvcEvent(ctx, svcProto)
		if err != nil {
			log.Printf("[Uploader] Failed to UpdateSvcEvent for Service %s/%s: %v",
				svc.Namespace, svc.Name, err)
			return
		}
		log.Printf("[Uploader] handleServiceEvent: UPDATE Service %s/%s => Manager resp=%v",
			svc.Namespace, svc.Name, resp)

	case "DELETE":
		delSvcProto := &protobuf.Service{
			Cluster:   svcProto.Cluster,
			Namespace: svc.Namespace,
			Name:      svc.Name,
		}
		resp, err := upl.grpcClient.DeleteSvcEvent(ctx, delSvcProto)
		if err != nil {
			log.Printf("[Uploader] Failed to DeleteSvcEvent for Service %s/%s: %v",
				svc.Namespace, svc.Name, err)
			return
		}
		log.Printf("[Uploader] handleServiceEvent: DELETE Service %s/%s => Manager resp=%v",
			svc.Namespace, svc.Name, resp)

	default:
		log.Printf("[Uploader] handleServiceEvent: Unrecognized action=%s for Service %s/%s",
			action, svc.Namespace, svc.Name)
	}
}

// handleDeployEvent Function
func (upl *UplHandler) handleDeployEvent(action string, obj interface{}) {
	dep, ok := obj.(*appsv1.Deployment)
	if !ok {
		log.Printf("[Uploader] handleDeployEvent: Not a *appsv1.Deployment object")
		return
	}
	if upl.grpcClient == nil {
		return
	}

	depProto := convertDeploymentToProto(dep)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch action {
	case "ADD":
		resp, err := upl.grpcClient.AddDeployEvent(ctx, depProto)
		if err != nil {
			log.Printf("[Uploader] Failed to AddDeployEvent for Deployment %s/%s: %v",
				dep.Namespace, dep.Name, err)
			return
		}
		log.Printf("[Uploader] handleDeployEvent: ADD Deploy %s/%s => Manager resp=%v",
			dep.Namespace, dep.Name, resp)

	case "UPDATE":
		resp, err := upl.grpcClient.UpdateDeployEvent(ctx, depProto)
		if err != nil {
			log.Printf("[Uploader] Failed to UpdateDeployEvent for Deployment %s/%s: %v",
				dep.Namespace, dep.Name, err)
			return
		}
		log.Printf("[Uploader] handleDeployEvent: UPDATE Deploy %s/%s => Manager resp=%v",
			dep.Namespace, dep.Name, resp)

	case "DELETE":
		delDepProto := &protobuf.Deploy{
			Cluster:   depProto.Cluster,
			Namespace: dep.Namespace,
			Name:      dep.Name,
		}
		resp, err := upl.grpcClient.DeleteDeployEvent(ctx, delDepProto)
		if err != nil {
			log.Printf("[Uploader] Failed to DeleteDeployEvent for Deploy %s/%s: %v",
				dep.Namespace, dep.Name, err)
			return
		}
		log.Printf("[Uploader] handleDeployEvent: DELETE Deploy %s/%s => Manager resp=%v",
			dep.Namespace, dep.Name, resp)

	default:
		log.Printf("[Uploader] handleDeployEvent: Unrecognized action=%s for Deploy %s/%s",
			action, dep.Namespace, dep.Name)
	}
}

// convertPodToProto Function
func convertPodToProto(pod *corev1.Pod) *protobuf.Pod {
	return &protobuf.Pod{
		Cluster:           config.GlobalConfig.ClusterName,
		Namespace:         pod.Namespace,
		Name:              pod.Name,
		NodeName:          pod.Spec.NodeName,
		PodIP:             pod.Status.PodIP,
		Status:            string(pod.Status.Phase),
		Labels:            pod.Labels,
		CreationTimestamp: pod.CreationTimestamp.String(),
	}
}

// convertServiceToProto Function
func convertServiceToProto(svc *corev1.Service) *protobuf.Service {
	var protoPorts []*protobuf.Port
	for _, p := range svc.Spec.Ports {
		protoPorts = append(protoPorts, &protobuf.Port{
			Port:       int32(p.Port),
			TargetPort: p.TargetPort.IntVal,
			Protocol:   string(p.Protocol),
		})
	}

	var externalIPs []string
	if len(svc.Spec.ExternalIPs) > 0 {
		externalIPs = append(externalIPs, svc.Spec.ExternalIPs...)
	}

	var lbIngresses []string
	for _, ing := range svc.Status.LoadBalancer.Ingress {
		if ing.IP != "" {
			lbIngresses = append(lbIngresses, ing.IP)
		} else if ing.Hostname != "" {
			lbIngresses = append(lbIngresses, ing.Hostname)
		}
	}

	return &protobuf.Service{
		Cluster:         config.GlobalConfig.ClusterName,
		Namespace:       svc.Namespace,
		Name:            svc.Name,
		Type:            string(svc.Spec.Type),
		ClusterIP:       svc.Spec.ClusterIP,
		Ports:           protoPorts,
		Labels:          svc.Labels,
		ExternalIPs:     externalIPs,
		LoadBalancerIPs: lbIngresses,
	}
}

// convertDeploymentToProto Function
func convertDeploymentToProto(dep *appsv1.Deployment) *protobuf.Deploy {
	replicas := int32(0)
	if dep.Spec.Replicas != nil {
		replicas = int32(*dep.Spec.Replicas)
	}
	return &protobuf.Deploy{
		Cluster:           config.GlobalConfig.ClusterName,
		Namespace:         dep.Namespace,
		Name:              dep.Name,
		DesiredReplicas:   replicas,
		AvailableReplicas: dep.Status.AvailableReplicas,
		Labels:            dep.Labels,
		CreationTimestamp: dep.CreationTimestamp.String(),
	}
}
