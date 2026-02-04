# KubeCarto

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.24-blue.svg)](https://golang.org/)
[![Istio](https://img.shields.io/badge/Istio-Service%20Mesh-blue.svg)](https://istio.io/)

KubeCarto is an Istio/Envoy-based Kubernetes observability system that collects API logs, Envoy metrics, and Kubernetes resource events across clusters in real time via gRPC streaming.

## Collected Data

| Data Type | Description |
|-----------|-------------|
| API Logs | HTTP request/response logs captured from Istio/Envoy sidecars (method, path, response code, src/dst metadata) |
| Envoy Metrics | Envoy proxy metrics aggregated per pod with label-based filtering |
| K8s Events (Deploy) | Deployment add/update/delete events with replica counts and labels |
| K8s Events (Pod) | Pod add/update/delete events with node, IP, status, and labels |
| K8s Events (Service) | Service add/update/delete events with type, ClusterIP, ports, and labels |

## Deployment

### Prerequisites

- Kubernetes cluster (v1.28+)
- Istio service mesh installed on the cluster

### Kubernetes Deployment

```bash
# 1. Namespace, ServiceAccount, RBAC
kubectl apply -f deployments/kubecarto.yaml

# 2. Manager Deployment + Service
kubectl apply -f deployments/kubecarto-manager.yaml

# 3. Agent Deployment + Service
kubectl apply -f deployments/kubecarto-agent.yaml

# 4. Log Client (optional, for stdout observation)
kubectl apply -f deployments/log-client.yaml
```

### Build from Source

```bash
# Build all binaries (agent + manager)
make build

# Or build individually
cd kubecarto
go build -o ../bin/agent ./agent/main.go
go build -o ../bin/manager ./manager/main.go
```

### Docker Build

```bash
# Build all images
make docker

# Or build individually
docker build --target agent -t boanlab/kubecarto-agent:v0.1 -f kubecarto/agent/Dockerfile .
docker build --target manager -t boanlab/kubecarto-manager:v0.1 -f kubecarto/manager/Dockerfile .
```

## Development

### Prerequisites

- Go 1.24+
- protoc (Protocol Buffers compiler)

### Project Structure

```
KubeCarto/
├── Makefile
├── kubecarto/
│   ├── go.mod
│   ├── protobuf/          # gRPC proto and generated code
│   ├── types/             # Shared type definitions
│   ├── agent/
│   │   ├── main.go
│   │   ├── Dockerfile
│   │   ├── config/        # Agent configuration (flags + env vars)
│   │   ├── core/          # Agent lifecycle
│   │   ├── collector/     # OpenTelemetry and Envoy metric collection
│   │   ├── processor/     # API log and metric processing
│   │   ├── uploader/      # gRPC streaming to manager
│   │   └── k8s/           # Kubernetes informers and Istio patching
│   └── manager/
│       ├── main.go
│       ├── Dockerfile
│       ├── config/        # Manager configuration (flags + env vars)
│       ├── core/          # Manager lifecycle
│       ├── collector/     # gRPC server receiving agent streams
│       └── exporter/      # gRPC server exporting to clients
├── clients/
│   ├── log-client/        # Stdout/file log client
│   └── mongo-client/      # MongoDB storage client
└── deployments/
    ├── kubecarto.yaml          # Namespace, SA, RBAC
    ├── kubecarto-manager.yaml  # Manager Deployment + Service
    ├── kubecarto-agent.yaml    # Agent Deployment + Service
    ├── log-client.yaml         # Log client Deployment
    └── mongo-client.yaml       # MongoDB + Mongo client Deployment
```

### Configuration

**Agent** (`--collectorAddr`, `--collectorPort`, `--managerAddr`, `--managerPort`, `--clusterName`, ...):

| Flag / Env Var | Default | Description |
|----------------|---------|-------------|
| `--collectorAddr` / `COLLECTORADDR` | `0.0.0.0` | Address for Collector gRPC |
| `--collectorPort` / `COLLECTORPORT` | `4317` | Port for Collector gRPC |
| `--managerAddr` / `MANAGERADDR` | `kubecarto-manager.kubecarto.svc.cluster.local` | Address for Manager gRPC |
| `--managerPort` / `MANAGERPORT` | `5317` | Port for Manager gRPC |
| `--clusterName` / `CLUSTERNAME` | `UnKnown` | Name of the Kubernetes cluster |
| `--patchingNamespaces` / `PATCHINGNAMESPACES` | `false` | Enable patching `istio-injection` to all namespaces |
| `--restartingPatchedDeployments` / `RESTARTINGPATCHEDDEPLOYMENTS` | `false` | Enable restarting deployments after patching |
| `--aggregationPeriod` / `AGGREGATIONPERIOD` | `1` | Period for aggregating metrics (seconds) |
| `--cleanUpPeriod` / `CLEANUPPERIOD` | `5` | Period for cleaning up outdated metrics (seconds) |
| `--debug` / `DEBUG` | `false` | Enable debugging mode |

**Manager** (`--collectorAddr`, `--collectorPort`, `--exporterAddr`, `--exporterPort`, ...):

| Flag / Env Var | Default | Description |
|----------------|---------|-------------|
| `--collectorAddr` / `COLLECTORADDR` | `0.0.0.0` | Address for Collector gRPC (agent-facing) |
| `--collectorPort` / `COLLECTORPORT` | `5317` | Port for Collector gRPC (agent-facing) |
| `--exporterAddr` / `EXPORTERADDR` | `0.0.0.0` | Address for Exporter gRPC (client-facing) |
| `--exporterPort` / `EXPORTERPORT` | `8080` | Port for Exporter gRPC (client-facing) |
| `--aggregationPeriod` / `AGGREGATIONPERIOD` | `1` | Period for aggregating metrics (seconds) |
| `--cleanUpPeriod` / `CLEANUPPERIOD` | `5` | Period for cleaning up outdated metrics (seconds) |
| `--debug` / `DEBUG` | `false` | Enable debugging mode |

## gRPC Streaming API

KubeCarto uses bidirectional gRPC streaming: agents stream data to the manager, and the manager streams data to clients.

```protobuf
service KubeCarto {
  // manager -> client (server-side streaming)
  rpc GetAPILog(ClientInfo) returns (stream APILog);
  rpc GetEnvoyMetrics(ClientInfo) returns (stream EnvoyMetrics);

  rpc AddDeployEventDB(ClientInfo) returns (stream Deploy);
  rpc UpdateDeployEventDB(ClientInfo) returns (stream Deploy);
  rpc DeleteDeployEventDB(ClientInfo) returns (stream Deploy);

  rpc AddPodEventDB(ClientInfo) returns (stream Pod);
  rpc UpdatePodEventDB(ClientInfo) returns (stream Pod);
  rpc DeletePodEventDB(ClientInfo) returns (stream Pod);

  rpc AddSvcEventDB(ClientInfo) returns (stream Service);
  rpc UpdateSvcEventDB(ClientInfo) returns (stream Service);
  rpc DeleteSvcEventDB(ClientInfo) returns (stream Service);

  // agent -> manager (client-side streaming + unary)
  rpc GiveAPILog(stream APILog) returns (Response);
  rpc GiveEnvoyMetrics(stream EnvoyMetrics) returns (Response);

  rpc AddDeployEvent(Deploy) returns (Response);
  rpc UpdateDeployEvent(Deploy) returns (Response);
  rpc DeleteDeployEvent(Deploy) returns (Response);

  rpc AddPodEvent(Pod) returns (Response);
  rpc UpdatePodEvent(Pod) returns (Response);
  rpc DeletePodEvent(Pod) returns (Response);

  rpc AddSvcEvent(Service) returns (Response);
  rpc UpdateSvcEvent(Service) returns (Response);
  rpc DeleteSvcEvent(Service) returns (Response);
}
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

**Copyright 2025 [BoanLab](https://boanlab.com) @ DKU**
