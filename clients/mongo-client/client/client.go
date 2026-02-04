// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package client

import (
	"context"
	"log"
	"mongo-client/mongodb"

	pb "kubecarto/protobuf"
)

// Feeder Structure
type Feeder struct {
	Running bool

	client            pb.KubeCartoClient
	logStream         pb.KubeCarto_GetAPILogClient
	envoyMetricStream pb.KubeCarto_GetEnvoyMetricsClient

	deployAddStream    pb.KubeCarto_AddDeployEventDBClient
	deployUpdateStream pb.KubeCarto_UpdateDeployEventDBClient
	deployDeleteStream pb.KubeCarto_DeleteDeployEventDBClient

	podAddStream    pb.KubeCarto_AddPodEventDBClient
	podUpdateStream pb.KubeCarto_UpdatePodEventDBClient
	podDeleteStream pb.KubeCarto_DeletePodEventDBClient

	svcAddStream    pb.KubeCarto_AddSvcEventDBClient
	svcUpdateStream pb.KubeCarto_UpdateSvcEventDBClient
	svcDeleteStream pb.KubeCarto_DeleteSvcEventDBClient

	dbHandler mongodb.DBHandler

	Done chan struct{}
}

// NewClient Function
func NewClient(client pb.KubeCartoClient, clientInfo *pb.ClientInfo, logCfg string, metricCfg string, metricFilter string, mongoDBAddr string) *Feeder {
	fd := &Feeder{}
	fd.Running = true
	fd.client = client
	fd.Done = make(chan struct{})

	// === APILog ===
	if logCfg != "none" {
		logStream, err := client.GetAPILog(context.Background(), clientInfo)
		if err != nil {
			log.Fatalf("[Client] Could not get API log stream: %v", err)
		} else {
			fd.logStream = logStream
		}
	}

	// === EnvoyMetrics ===
	if metricCfg != "none" && (metricFilter == "all" || metricFilter == "envoy") {
		emStream, err := client.GetEnvoyMetrics(context.Background(), clientInfo)
		if err != nil {
			log.Fatalf("[Client] Could not get Envoy metrics stream: %v", err)
		} else {
			fd.envoyMetricStream = emStream
		}
	}

	// ========== Deploy Add/Update/Delete ==========
	if addDepStr, err := client.AddDeployEventDB(context.Background(), clientInfo); err != nil {
		log.Fatalf("[Client] Could not get AddDeployEventDB stream: %v", err)
	} else {
		fd.deployAddStream = addDepStr
	}

	if updDepStr, err := client.UpdateDeployEventDB(context.Background(), clientInfo); err != nil {
		log.Fatalf("[Client] Could not get UpdateDeployEventDB stream: %v", err)
	} else {
		fd.deployUpdateStream = updDepStr
	}

	if delDepStr, err := client.DeleteDeployEventDB(context.Background(), clientInfo); err != nil {
		log.Fatalf("[Client] Could not get DeleteDeployEventDB stream: %v", err)
	} else {
		fd.deployDeleteStream = delDepStr
	}

	// ========== Pod Add/Update/Delete ==========
	if addPodStr, err := client.AddPodEventDB(context.Background(), clientInfo); err != nil {
		log.Fatalf("[Client] Could not get AddPodEventDB stream: %v", err)
	} else {
		fd.podAddStream = addPodStr
	}

	if updPodStr, err := client.UpdatePodEventDB(context.Background(), clientInfo); err != nil {
		log.Fatalf("[Client] Could not get UpdatePodEventDB stream: %v", err)
	} else {
		fd.podUpdateStream = updPodStr
	}

	if delPodStr, err := client.DeletePodEventDB(context.Background(), clientInfo); err != nil {
		log.Fatalf("[Client] Could not get DeletePodEventDB stream: %v", err)
	} else {
		fd.podDeleteStream = delPodStr
	}

	// ========== Service Add/Update/Delete ==========
	if addSvcStr, err := client.AddSvcEventDB(context.Background(), clientInfo); err != nil {
		log.Fatalf("[Client] Could not get AddSvcEventDB stream: %v", err)
	} else {
		fd.svcAddStream = addSvcStr
	}

	if updSvcStr, err := client.UpdateSvcEventDB(context.Background(), clientInfo); err != nil {
		log.Fatalf("[Client] Could not get UpdateSvcEventDB stream: %v", err)
	} else {
		fd.svcUpdateStream = updSvcStr
	}

	if delSvcStr, err := client.DeleteSvcEventDB(context.Background(), clientInfo); err != nil {
		log.Fatalf("[Client] Could not get DeleteSvcEventDB stream: %v", err)
	} else {
		fd.svcDeleteStream = delSvcStr
	}

	// ========== MongoDB 연결 ==========
	dbHandler, err := mongodb.NewMongoDBHandler(mongoDBAddr)
	if err != nil {
		log.Fatalf("[MongoDB] Unable to initialize DB: %v", err)
	} else {
		fd.dbHandler = *dbHandler
	}

	return fd
}

// ========== Routines ========== //

// APILogRoutine Function
func (fd *Feeder) APILogRoutine(logCfg string) {
	if fd.logStream == nil {
		log.Printf("[APILogRoutine] logStream is nil, cannot receive logs.")
		return
	}

	for fd.Running {
		select {
		default:
			data, err := fd.logStream.Recv()
			if err != nil {
				log.Fatalf("[Client] Failed to receive an API log: %v", err)
				return
			}
			err = fd.dbHandler.InsertAPILog(data)
			if err != nil {
				log.Printf("[MongoDB] Failed to insert an API log: %v", err)
			} else {
				log.Printf("[MongoDB] Successfully inserted API log with ID: %d", data.Id)
			}
		case <-fd.Done:
			return
		}
	}
}

// EnvoyMetricsRoutine Function
func (fd *Feeder) EnvoyMetricsRoutine(metricCfg string) {
	if fd.envoyMetricStream == nil {
		log.Printf("[EnvoyMetricsRoutine] envoyMetricStream is nil, cannot receive metrics.")
		return
	}

	for fd.Running {
		select {
		default:
			data, err := fd.envoyMetricStream.Recv()
			if err != nil {
				log.Fatalf("[Client] Failed to receive Envoy metrics: %v", err)
				return
			}
			err = fd.dbHandler.InsertEnvoyMetrics(data)
			if err != nil {
				log.Printf("[MongoDB] Failed to insert Envoy metrics: %v", err)
			} else {
				log.Printf("[MongoDB] Successfully inserted Envoy metrics at timestamp: %s", data.TimeStamp)
			}
		case <-fd.Done:
			return
		}
	}
}

// DeployAddRoutine Function
func (fd *Feeder) DeployAddRoutine() {
	if fd.deployAddStream == nil {
		log.Printf("[DeployAddRoutine] deployAddStream is nil.")
		return
	}

	for fd.Running {
		select {
		default:
			dep, err := fd.deployAddStream.Recv()
			if err != nil {
				log.Fatalf("[Client] DeployAdd stream ended: %v", err)
				return
			}
			if err := fd.dbHandler.InsertDeploy(dep); err != nil {
				log.Printf("[MongoDB] InsertDeploy(Add) error: %v", err)
			} else {
				log.Printf("[MongoDB] Successfully inserted deploy event for: %s", dep.Name)
			}
		case <-fd.Done:
			return
		}
	}
}

// DeployUpdateRoutine Function
func (fd *Feeder) DeployUpdateRoutine() {
	if fd.deployUpdateStream == nil {
		log.Printf("[DeployUpdateRoutine] deployUpdateStream is nil.")
		return
	}

	for fd.Running {
		select {
		default:
			dep, err := fd.deployUpdateStream.Recv()
			if err != nil {
				log.Fatalf("[Client] DeployUpdate stream ended: %v", err)
				return
			}
			if err := fd.dbHandler.UpdateDeploy(dep); err != nil {
				log.Printf("[MongoDB] UpdateDeploy error: %v", err)
			} else {
				log.Printf("[MongoDB] Successfully updated deploy event for: %s", dep.Name)
			}
		case <-fd.Done:
			return
		}
	}
}

// DeployDeleteRoutine Function
func (fd *Feeder) DeployDeleteRoutine() {
	if fd.deployDeleteStream == nil {
		log.Printf("[DeployDeleteRoutine] deployDeleteStream is nil.")
		return
	}

	for fd.Running {
		select {
		default:
			dep, err := fd.deployDeleteStream.Recv()
			if err != nil {
				log.Fatalf("[Client] DeployDelete stream ended: %v", err)
				return
			}
			if err := fd.dbHandler.DeleteDeploy(dep); err != nil {
				log.Printf("[MongoDB] DeleteDeploy error: %v", err)
			} else {
				log.Printf("[MongoDB] Successfully deleted deploy event for: %s", dep.Name)
			}
		case <-fd.Done:
			return
		}
	}
}

// PodAddRoutine Function
func (fd *Feeder) PodAddRoutine() {
	if fd.podAddStream == nil {
		log.Printf("[PodAddRoutine] podAddStream is nil.")
		return
	}

	for fd.Running {
		select {
		default:
			pod, err := fd.podAddStream.Recv()
			if err != nil {
				log.Fatalf("[Client] PodAdd stream ended: %v", err)
				return
			}
			if err := fd.dbHandler.InsertPod(pod); err != nil {
				log.Printf("[MongoDB] InsertPod(Add) error: %v", err)
			} else {
				log.Printf("[MongoDB] Successfully inserted pod event for: %s", pod.Name)
			}
		case <-fd.Done:
			return
		}
	}
}

// PodUpdateRoutine Function
func (fd *Feeder) PodUpdateRoutine() {
	if fd.podUpdateStream == nil {
		log.Printf("[PodUpdateRoutine] podUpdateStream is nil.")
		return
	}

	for fd.Running {
		select {
		default:
			pod, err := fd.podUpdateStream.Recv()
			if err != nil {
				log.Fatalf("[Client] PodUpdate stream ended: %v", err)
				return
			}
			if err := fd.dbHandler.UpdatePod(pod); err != nil {
				log.Printf("[MongoDB] UpdatePod error: %v", err)
			} else {
				log.Printf("[MongoDB] Successfully updated pod event for: %s", pod.Name)
			}
		case <-fd.Done:
			return
		}
	}
}

// PodDeleteRoutine Function
func (fd *Feeder) PodDeleteRoutine() {
	if fd.podDeleteStream == nil {
		log.Printf("[PodDeleteRoutine] podDeleteStream is nil.")
		return
	}

	for fd.Running {
		select {
		default:
			pod, err := fd.podDeleteStream.Recv()
			if err != nil {
				log.Fatalf("[Client] PodDelete stream ended: %v", err)
				return
			}
			if err := fd.dbHandler.DeletePod(pod); err != nil {
				log.Printf("[MongoDB] DeletePod error: %v", err)
			} else {
				log.Printf("[MongoDB] Successfully deleted pod event for: %s", pod.Name)
			}
		case <-fd.Done:
			return
		}
	}
}

// ServiceAddRoutine Function
func (fd *Feeder) ServiceAddRoutine() {
	if fd.svcAddStream == nil {
		log.Printf("[ServiceAddRoutine] svcAddStream is nil.")
		return
	}

	for fd.Running {
		select {
		default:
			svc, err := fd.svcAddStream.Recv()
			if err != nil {
				log.Fatalf("[Client] ServiceAdd stream ended: %v", err)
				return
			}
			if err := fd.dbHandler.InsertService(svc); err != nil {
				log.Printf("[MongoDB] InsertService(Add) error: %v", err)
			} else {
				log.Printf("[MongoDB] Successfully inserted service event for: %s", svc.Name)
			}
		case <-fd.Done:
			return
		}
	}
}

// ServiceUpdateRoutine Function
func (fd *Feeder) ServiceUpdateRoutine() {
	if fd.svcUpdateStream == nil {
		log.Printf("[ServiceUpdateRoutine] svcUpdateStream is nil.")
		return
	}

	for fd.Running {
		select {
		default:
			svc, err := fd.svcUpdateStream.Recv()
			if err != nil {
				log.Fatalf("[Client] ServiceUpdate stream ended: %v", err)
				return
			}
			if err := fd.dbHandler.UpdateService(svc); err != nil {
				log.Printf("[MongoDB] UpdateService error: %v", err)
			} else {
				log.Printf("[MongoDB] Successfully updated service event for: %s", svc.Name)
			}
		case <-fd.Done:
			return
		}
	}
}

// ServiceDeleteRoutine Function
func (fd *Feeder) ServiceDeleteRoutine() {
	if fd.svcDeleteStream == nil {
		log.Printf("[ServiceDeleteRoutine] svcDeleteStream is nil.")
		return
	}

	for fd.Running {
		select {
		default:
			svc, err := fd.svcDeleteStream.Recv()
			if err != nil {
				log.Fatalf("[Client] ServiceDelete stream ended: %v", err)
				return
			}
			if err := fd.dbHandler.DeleteService(svc); err != nil {
				log.Printf("[MongoDB] DeleteService error: %v", err)
			} else {
				log.Printf("[MongoDB] Successfully deleted service event for: %s", svc.Name)
			}
		case <-fd.Done:
			return
		}
	}
}
