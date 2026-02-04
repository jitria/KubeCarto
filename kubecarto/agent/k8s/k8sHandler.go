// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package k8s

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"k8s.io/apimachinery/pkg/util/json"

	"kubecarto/agent/config"
	"kubecarto/types"
	"kubecarto/agent/uploader"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// K8sH global reference for Kubernetes Handler
var K8sH *KubernetesHandler

// init Function
func init() {
	K8sH = NewK8sHandler()
}

// KubernetesHandler Structure
type KubernetesHandler struct {
	config    *rest.Config
	clientSet *kubernetes.Clientset

	watchers  map[string]*cache.ListWatch
	informers map[string]cache.Controller

	podMap     map[string]*corev1.Pod        // NOT thread safe, key: Pod IP
	serviceMap map[string]*corev1.Service    // NOT thread safe, key: Service IP
	deployMap  map[string]*appsv1.Deployment // NOT thread safe, key: Namespace/DeploymentName
}

// NewK8sHandler Function
func NewK8sHandler() *KubernetesHandler {
	kh := &KubernetesHandler{
		watchers:  make(map[string]*cache.ListWatch),
		informers: make(map[string]cache.Controller),

		podMap:     make(map[string]*corev1.Pod),
		serviceMap: make(map[string]*corev1.Service),
		deployMap:  make(map[string]*appsv1.Deployment),
	}

	return kh
}

// InitK8sClient Function
func InitK8sClient() bool {
	var err error

	// Initialize in cluster config
	K8sH.config, err = rest.InClusterConfig()
	if err != nil {
		log.Print("[InitK8sClient] Failed to initialize Kubernetes client")
		return false
	}

	// Initialize Kubernetes clientSet
	K8sH.clientSet, err = kubernetes.NewForConfig(K8sH.config)
	if err != nil {
		log.Print("[InitK8sClient] Failed to initialize Kubernetes client")
		return false
	}

	watchTargetsCoreV1 := []string{"pods", "services"}
	watchTargetsAppsV1 := []string{"deployments"}

	//  Initialize watchers for pods and services
	for _, target := range watchTargetsCoreV1 {
		watcher := cache.NewListWatchFromClient(
			K8sH.clientSet.CoreV1().RESTClient(),
			target,
			corev1.NamespaceAll,
			fields.Everything(),
		)
		K8sH.watchers[target] = watcher
	}

	// Initialize watchers for deployments
	for _, target := range watchTargetsAppsV1 {
		watcher := cache.NewListWatchFromClient(
			K8sH.clientSet.AppsV1().RESTClient(),
			target,
			corev1.NamespaceAll,
			fields.Everything(),
		)
		K8sH.watchers[target] = watcher
	}

	// Initialize informers
	K8sH.initInformers()

	log.Print("[InitK8sClient] Initialized Kubernetes client")

	return true
}

// initInformers Function that initializes informers for services and pods in a cluster
func (k8s *KubernetesHandler) initInformers() {
	// Create Pod controller informer
	if k8s.watchers["pods"] != nil {
		_, podController := cache.NewInformerWithOptions(
			cache.InformerOptions{
				ListerWatcher: k8s.watchers["pods"],
				ObjectType:    &corev1.Pod{},
				ResyncPeriod:  0,
				Handler: cache.ResourceEventHandlerFuncs{
					AddFunc: func(obj interface{}) {
						pod := obj.(*corev1.Pod)
						ip := pod.Status.PodIP
						if ip != "" {
							k8s.podMap[ip] = pod
							log.Printf("[Informer:Pod] ADDED Pod %s/%s, IP=%s", pod.Namespace, pod.Name, ip)
							go uploader.UplH.UploadClusterEvent("Pod", "ADD", pod)
						}
					},
					UpdateFunc: func(oldObj, newObj interface{}) {
						newPod := newObj.(*corev1.Pod)
						ip := newPod.Status.PodIP
						if ip != "" {
							k8s.podMap[ip] = newPod
							log.Printf("[Informer:Pod] UPDATED Pod %s/%s, IP=%s", newPod.Namespace, newPod.Name, ip)
							go uploader.UplH.UploadClusterEvent("Pod", "UPDATE", newPod)
						}
					},
					DeleteFunc: func(obj interface{}) {
						pod := obj.(*corev1.Pod)
						ip := pod.Status.PodIP
						if ip != "" {
							delete(k8s.podMap, ip)
							log.Printf("[Informer:Pod] DELETED Pod %s/%s, IP=%s", pod.Namespace, pod.Name, ip)
							go uploader.UplH.UploadClusterEvent("Pod", "DELETE", pod)
						}
					},
				},
			},
		)
		k8s.informers["pods"] = podController
	}

	// Create Service controller informer
	if k8s.watchers["services"] != nil {
		_, svcController := cache.NewInformerWithOptions(
			cache.InformerOptions{
				ListerWatcher: k8s.watchers["services"],
				ObjectType:    &corev1.Service{},
				ResyncPeriod:  0,
				Handler: cache.ResourceEventHandlerFuncs{
					AddFunc: func(obj interface{}) {
						svc := obj.(*corev1.Service)
						k8s.addOrUpdateServiceIPs(svc)
						log.Printf("[Informer:Service] ADDED Service %s/%s", svc.Namespace, svc.Name)
						go uploader.UplH.UploadClusterEvent("Service", "ADD", svc)
					},
					UpdateFunc: func(oldObj, newObj interface{}) {
						oldSvc := oldObj.(*corev1.Service)
						newSvc := newObj.(*corev1.Service)
						k8s.removeServiceIPs(oldSvc)
						k8s.addOrUpdateServiceIPs(newSvc)
						log.Printf("[Informer:Service] UPDATED Service %s/%s", newSvc.Namespace, newSvc.Name)
						go uploader.UplH.UploadClusterEvent("Service", "UPDATE", newSvc)
					},
					DeleteFunc: func(obj interface{}) {
						svc := obj.(*corev1.Service)
						k8s.removeServiceIPs(svc)
						log.Printf("[Informer:Service] DELETED Service %s/%s", svc.Namespace, svc.Name)
						go uploader.UplH.UploadClusterEvent("Service", "DELETE", svc)
					},
				},
			},
		)
		k8s.informers["services"] = svcController
	}

	// Create Deployment controller informer
	if k8s.watchers["deployments"] != nil {
		_, depController := cache.NewInformerWithOptions(
			cache.InformerOptions{
				ListerWatcher: k8s.watchers["deployments"],
				ObjectType:    &appsv1.Deployment{},
				ResyncPeriod:  0,
				Handler: cache.ResourceEventHandlerFuncs{
					AddFunc: func(obj interface{}) {
						dep := obj.(*appsv1.Deployment)
						key := fmt.Sprintf("%s/%s", dep.Namespace, dep.Name)
						k8s.deployMap[key] = dep
						log.Printf("[Informer:Deploy] ADDED Deployment %s", key)
						go uploader.UplH.UploadClusterEvent("Deploy", "ADD", dep)
					},
					UpdateFunc: func(oldObj, newObj interface{}) {
						dep := newObj.(*appsv1.Deployment)
						key := fmt.Sprintf("%s/%s", dep.Namespace, dep.Name)
						k8s.deployMap[key] = dep
						log.Printf("[Informer:Deploy] UPDATED Deployment %s", key)
						go uploader.UplH.UploadClusterEvent("Deploy", "UPDATE", dep)
					},
					DeleteFunc: func(obj interface{}) {
						dep := obj.(*appsv1.Deployment)
						key := fmt.Sprintf("%s/%s", dep.Namespace, dep.Name)
						delete(k8s.deployMap, key)
						log.Printf("[Informer:Deploy] DELETED Deployment %s", key)
						go uploader.UplH.UploadClusterEvent("Deploy", "DELETE", dep)
					},
				},
			},
		)
		k8s.informers["deployments"] = depController
	}
}

// addOrUpdateServiceIPs Function
func (k8s *KubernetesHandler) addOrUpdateServiceIPs(svc *corev1.Service) {
	if svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
		for _, lbIngress := range svc.Status.LoadBalancer.Ingress {
			lbIP := lbIngress.IP
			if lbIP != "" {
				k8s.serviceMap[lbIP] = svc
			}
		}
	} else {
		if svc.Spec.ClusterIP != "" && svc.Spec.ClusterIP != "None" {
			k8s.serviceMap[svc.Spec.ClusterIP] = svc
		}
		for _, eip := range svc.Spec.ExternalIPs {
			k8s.serviceMap[eip] = svc
		}
	}
}

// removeServiceIPs Function
func (k8s *KubernetesHandler) removeServiceIPs(svc *corev1.Service) {
	if svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
		for _, lbIngress := range svc.Status.LoadBalancer.Ingress {
			lbIP := lbIngress.IP
			if lbIP != "" {
				delete(k8s.serviceMap, lbIP)
			}
		}
	} else {
		if svc.Spec.ClusterIP != "" && svc.Spec.ClusterIP != "None" {
			delete(k8s.serviceMap, svc.Spec.ClusterIP)
		}
		for _, eip := range svc.Spec.ExternalIPs {
			delete(k8s.serviceMap, eip)
		}
	}
}

// RunInformers Function that starts running informers
func RunInformers(stopChan chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)

	go func() {
		defer wg.Done()
		for name, informer := range K8sH.informers {
			go func(name string, inf cache.Controller) {
				log.Printf("[RunInformers] Starting informer for %s", name)
				inf.Run(stopChan)
			}(name, informer)
		}
		<-stopChan
		log.Print("[RunInformers] Stop signal received. All informers stopping.")
	}()

	log.Print("[RunInformers] Started all Kubernetes informers")
}

// getConfigMap Function
func (k8s *KubernetesHandler) getConfigMap(namespace, name string) (string, error) {
	cm, err := k8s.clientSet.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, v1.GetOptions{})
	if err != nil {
		log.Printf("[K8s] Failed to get ConfigMaps: %v", err)
		return "", err
	}

	// convert data to string
	data, err := json.Marshal(cm.Data)
	if err != nil {
		log.Printf("[K8s] Failed to marshal ConfigMap: %v", err)
		return "", err
	}

	return string(data), nil
}

// updateConfigMap Function
func (k8s *KubernetesHandler) updateConfigMap(namespace, name, data string) error {
	cm, err := k8s.clientSet.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, v1.GetOptions{})
	if err != nil {
		log.Printf("[K8s] Failed to get ConfigMap: %v", err)
		return err
	}

	if _, ok := cm.Data["mesh"]; !ok {
		return errors.New("[K8s] Unable to find field \"mesh\" from Istio config")
	}

	cm.Data["mesh"] = data
	if _, err := k8s.clientSet.CoreV1().ConfigMaps(namespace).Update(context.Background(), cm, v1.UpdateOptions{}); err != nil {
		return err
	}

	return nil
}

// PatchNamespaces Function that patches namespaces for adding 'istio-injection'
func PatchNamespaces() bool {
	namespaces, err := K8sH.clientSet.CoreV1().Namespaces().List(context.Background(), v1.ListOptions{})
	if err != nil {
		log.Printf("[PatchNamespaces] Failed to get Namespaces: %v", err)
		return false
	}

	for _, ns := range namespaces.Items {
		namespace := ns.DeepCopy()

		// By default, we want to patch the following namespaces
		// namespace.Name == "kubecarto" => for multicluster networking

		// If don't want to patch the "default" namespace, following code can be used
		// if namespace.Name == "default" {
		// 	continue
		// }

		namespace.Labels["istio-injection"] = "enabled"

		// Patch the namespace
		if _, err := K8sH.clientSet.CoreV1().Namespaces().Update(context.TODO(), namespace, v1.UpdateOptions{FieldManager: "patcher"}); err != nil {
			log.Printf("[PatchNamespaces] Failed to update Namespace %s: %v", namespace.Name, err)
			return false
		}

		log.Printf("[PatchNamespaces] Updated Namespace %s", namespace.Name)
	}

	log.Print("[PatchNamespaces] Updated all Namespaces")

	return true
}

// restartDeployment Function that performs a rolling restart for a deployment in the specified namespace
// @todo: fix this, this DOES NOT restart deployments
func (k8s *KubernetesHandler) restartDeployment(namespace string, deploymentName string) error {
	deploymentClient := k8s.clientSet.AppsV1().Deployments(namespace)

	// Get the deployment to retrieve the current spec
	deployment, err := deploymentClient.Get(context.Background(), deploymentName, v1.GetOptions{})
	if err != nil {
		return err
	}

	// Trigger a rolling restart by updating the deployment's labels or annotations
	deployment.Spec.Template.ObjectMeta.Labels["restartedAt"] = v1.Now().String()

	// Update the deployment to trigger the rolling restart
	_, err = deploymentClient.Update(context.TODO(), deployment, v1.UpdateOptions{})
	if err != nil {
		return err
	}

	return nil
}

// RestartDeployments Function that restarts the deployments in the namespaces with "istio-injection=enabled"
func RestartDeployments() bool {
	deployments, err := K8sH.clientSet.AppsV1().Deployments("").List(context.Background(), v1.ListOptions{})
	if err != nil {
		log.Printf("[PatchDeployments] Failed to get Deployments: %v", err)
		return false
	}

	for _, deployment := range deployments.Items {
		// Skip the following namespaces
		if deployment.Namespace == "kubecarto" {
			continue
		}

		// Restart the deployment
		if err := K8sH.restartDeployment(deployment.Namespace, deployment.Name); err != nil {
			log.Printf("[PatchDeployments] Failed to restart Deployment %s/%s: %v", deployment.Namespace, deployment.Name, err)
			return false
		}

		log.Printf("[PatchDeployments] Deployment %s/%s restarted", deployment.Namespace, deployment.Name)
	}

	log.Print("[PatchDeployments] Restarted all patched deployments")

	return true
}

// lookupIPAddress Function
func lookupIPAddress(ipAddr string) interface{} {
	// Look for pod map
	pod, ok := K8sH.podMap[ipAddr]
	if ok {
		return pod
	}

	// Look for service map
	service, ok := K8sH.serviceMap[ipAddr]
	if ok {
		return service
	}

	return nil
}

// LookupK8sResource Function
func LookupK8sResource(srcIP string) types.K8sResource {
	ret := types.K8sResource{
		Cluster:   "Unknown",
		Namespace: "Unknown",
		Name:      "Unknown",
		Labels:    make(map[string]string),
		Type:      types.K8sResourceTypeUnknown,
	}

	// Find Kubernetes resource from source IP (service or a pod)
	raw := lookupIPAddress(srcIP)

	// Currently supports Service or Pod
	switch raw.(type) {
	case *corev1.Pod:
		pod, ok := raw.(*corev1.Pod)
		if ok {
			ret.Cluster = config.GlobalConfig.ClusterName
			ret.Namespace = pod.Namespace
			ret.Name = pod.Name
			ret.Labels = pod.Labels
			ret.Type = types.K8sResourceTypePod
		}
	case *corev1.Service:
		svc, ok := raw.(*corev1.Service)
		if ok {
			ret.Cluster = config.GlobalConfig.ClusterName
			ret.Namespace = svc.Namespace
			ret.Name = svc.Name
			ret.Labels = svc.Labels
			ret.Type = types.K8sResourceTypeService
		}
	default: // Unknown resource
		ret.Type = types.K8sResourceTypeUnknown
	}

	return ret
}
