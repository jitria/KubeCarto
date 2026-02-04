// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package types

// K8sResourceTypes
const (
	K8sResourceTypeUnknown = 0
	K8sResourceTypePod     = 1
	K8sResourceTypeService = 2
)

type ClusterEvent struct {
	ResourceType string      // "Pod" / "Service" / "Deploy"
	Action       string      // "ADD", "UPDATE", "DELETE"
	Object       interface{} // *corev1.Pod, *corev1.Service, *appsv1.Deployment
}

// K8sResource Structure
type K8sResource struct {
	Cluster    string
	Type       uint8
	Namespace  string
	Name       string
	Labels     map[string]string
	Containers []string
}

// K8sResourceTypeToString Function
func K8sResourceTypeToString(resourceType uint8) string {
	switch resourceType {
	case K8sResourceTypePod:
		return "Pod"
	case K8sResourceTypeService:
		return "Service"
	case K8sResourceTypeUnknown:
		return "Unknown"
	}
	return "Unknown"
}
