# Getting Started with KubeCarto

This guide will help you quickly get started with KubeCarto and experience its core features in just a few minutes.

## What is KubeCarto?

KubeCarto is an Istio/Envoy-based Kubernetes observability system that collects and streams cluster-wide monitoring data in real time. It provides:

- **API log collection** capturing HTTP request/response metadata (method, path, response code, source/destination) from Istio/Envoy sidecars
- **Envoy metrics aggregation** with per-pod metric collection and label-based filtering
- **Kubernetes event tracking** for Deployments, Pods, and Services (add/update/delete) with full resource metadata

## Quick Start

### Step 1: Deploy KubeCarto

Deploy KubeCarto to your Kubernetes cluster:

```bash
# Clone the repository
git clone https://github.com/BoanLab/KubeCarto.git
cd KubeCarto

# 1. Create Namespace, ServiceAccount, and RBAC
kubectl apply -f deployments/kubecarto.yaml

# 2. Deploy Manager
kubectl apply -f deployments/kubecarto-manager.yaml

# 3. Deploy Agent
kubectl apply -f deployments/kubecarto-agent.yaml

# 4. Deploy Log Client
kubectl apply -f deployments/log-client.yaml

# Wait for pods to be ready
kubectl wait --for=condition=ready pod -l app=kubecarto-manager -n kubecarto --timeout=60s
kubectl wait --for=condition=ready pod -l app=kubecarto-agent -n kubecarto --timeout=60s
kubectl wait --for=condition=ready pod -l app=log-client -n kubecarto --timeout=60s
```

### Step 2: Verify Connection

Check that the Manager and Agent are connected:

```bash
# Check Manager logs for incoming agent streams
kubectl logs -n kubecarto deployment/kubecarto-manager --tail=20

# Check Agent logs for connection to Manager
kubectl logs -n kubecarto deployment/kubecarto-agent --tail=20
```

### Step 3: Generate Test Traffic

Deploy a sample application with Istio sidecar injection and send HTTP requests:

```bash
# Create a test namespace with Istio injection
kubectl create namespace test-app
kubectl label namespace test-app istio-injection=enabled

# Deploy httpbin as a target service
kubectl run httpbin --image=kennethreitz/httpbin --port=80 -n test-app
kubectl expose pod httpbin --port=80 -n test-app

# Send test traffic from a sleep pod
kubectl run sleep --image=curlimages/curl --rm -it --restart=Never -n test-app -- \
  curl -s http://httpbin/get
```

### Step 4: Observe Output

Check the log-client output for collected data:

```bash
# View API logs and metrics streamed from the Manager
kubectl logs -n kubecarto deployment/log-client --tail=50
```

## Example: Monitoring Data

### API Logs

KubeCarto captures HTTP request/response logs from Istio/Envoy sidecars:

```
APILog | TimeStamp=2025-01-15T10:30:00Z | Src=test-app/sleep(10.244.1.5:43210) | Dst=test-app/httpbin(10.244.2.3:80) | Protocol=HTTP | Method=GET | Path=/get | ResponseCode=200
```

### Envoy Metrics

Envoy proxy metrics are aggregated per pod:

```
EnvoyMetrics | TimeStamp=2025-01-15T10:30:05Z | Namespace=test-app | Name=httpbin | IP=10.244.2.3 | Metrics={upstream_cx_total: 5, upstream_rq_200: 5}
```

### K8s Events

Kubernetes resource events are tracked in real time:

```
DeployEvent  | ADD    | Cluster=cluster1 | Namespace=test-app | Name=httpbin | Desired=1 | Available=1
PodEvent     | ADD    | Cluster=cluster1 | Namespace=test-app | Name=httpbin-abc123 | Node=worker1 | IP=10.244.2.3 | Status=Running
ServiceEvent | ADD    | Cluster=cluster1 | Namespace=test-app | Name=httpbin | Type=ClusterIP | ClusterIP=10.96.0.15 | Port=80/TCP
```

## Build from Source

```bash
cd KubeCarto

# Build all binaries (agent + manager)
make build

# Binaries are output to bin/
ls bin/
# agent  manager

# Run locally
./bin/agent --clusterName=cluster1 --managerAddr=<manager-ip> --managerPort=5317
./bin/manager --collectorAddr=0.0.0.0 --collectorPort=5317 --exporterAddr=0.0.0.0 --exporterPort=8080
```

## Cleanup

Remove KubeCarto from the cluster:

```bash
kubectl delete -f deployments/log-client.yaml
kubectl delete -f deployments/kubecarto-agent.yaml
kubectl delete -f deployments/kubecarto-manager.yaml
kubectl delete -f deployments/kubecarto.yaml
```

---

Need help? [Open an issue on GitHub](https://github.com/BoanLab/KubeCarto/issues).
