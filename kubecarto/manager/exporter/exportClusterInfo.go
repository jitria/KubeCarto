// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package exporter

import (
	"fmt"
	"log"
	"sync"

	"kubecarto/protobuf"
)

type deployAddStreamInform struct {
	Hostname  string
	IPAddress string
	stream    protobuf.KubeCarto_AddDeployEventDBServer
	errChan   chan error
}

type deployUpdateStreamInform struct {
	Hostname  string
	IPAddress string
	stream    protobuf.KubeCarto_UpdateDeployEventDBServer
	errChan   chan error
}

type deployDeleteStreamInform struct {
	Hostname  string
	IPAddress string
	stream    protobuf.KubeCarto_DeleteDeployEventDBServer
	errChan   chan error
}

type podAddStreamInform struct {
	Hostname  string
	IPAddress string
	stream    protobuf.KubeCarto_AddPodEventDBServer
	errChan   chan error
}

type podUpdateStreamInform struct {
	Hostname  string
	IPAddress string
	stream    protobuf.KubeCarto_UpdatePodEventDBServer
	errChan   chan error
}

type podDeleteStreamInform struct {
	Hostname  string
	IPAddress string
	stream    protobuf.KubeCarto_DeletePodEventDBServer
	errChan   chan error
}

type svcAddStreamInform struct {
	Hostname  string
	IPAddress string
	stream    protobuf.KubeCarto_AddSvcEventDBServer
	errChan   chan error
}

type svcUpdateStreamInform struct {
	Hostname  string
	IPAddress string
	stream    protobuf.KubeCarto_UpdateSvcEventDBServer
	errChan   chan error
}

type svcDeleteStreamInform struct {
	Hostname  string
	IPAddress string
	stream    protobuf.KubeCarto_DeleteSvcEventDBServer
	errChan   chan error
}

func InsertDeployAdd(dep *protobuf.Deploy) {
	ExpH.exporterDeployAdd <- dep
}

func InsertDeployUpdate(dep *protobuf.Deploy) {
	ExpH.exporterDeployUpdate <- dep
}

func InsertDeployDelete(dep *protobuf.Deploy) {
	ExpH.exporterDeployDelete <- dep
}

func InsertSvcAdd(svc *protobuf.Service) {
	ExpH.exporterSvcAdd <- svc
}

func InsertSvcUpdate(svc *protobuf.Service) {
	ExpH.exporterSvcUpdate <- svc
}

func InsertSvcDelete(svc *protobuf.Service) {
	ExpH.exporterSvcDelete <- svc
}

func InsertPodAdd(pod *protobuf.Pod) {
	ExpH.exporterPodAdd <- pod
}

func InsertPodUpdate(pod *protobuf.Pod) {
	ExpH.exporterPodUpdate <- pod
}

func InsertPodDelete(pod *protobuf.Pod) {
	ExpH.exporterPodDelete <- pod
}

// exportClusterHandler Function
func (exp *ExpHandler) exportClusterHandler(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		// Deploy
		case dep := <-exp.exporterDeployAdd:
			if dep != nil {
				exp.SendDeployAdd(dep)
			}
		case dep := <-exp.exporterDeployUpdate:
			if dep != nil {
				exp.SendDeployUpdate(dep)
			}
		case dep := <-exp.exporterDeployDelete:
			if dep != nil {
				exp.SendDeployDelete(dep)
			}

		// Pod
		case pod := <-exp.exporterPodAdd:
			if pod != nil {
				exp.SendPodAdd(pod)
			}
		case pod := <-exp.exporterPodUpdate:
			if pod != nil {
				exp.SendPodUpdate(pod)
			}
		case pod := <-exp.exporterPodDelete:
			if pod != nil {
				exp.SendPodDelete(pod)
			}

		// Service
		case svc := <-exp.exporterSvcAdd:
			if svc != nil {
				exp.SendServiceAdd(svc)
			}
		case svc := <-exp.exporterSvcUpdate:
			if svc != nil {
				exp.SendServiceUpdate(svc)
			}
		case svc := <-exp.exporterSvcDelete:
			if svc != nil {
				exp.SendServiceDelete(svc)
			}

		case <-exp.stopChan:
			log.Print("[Exporter] Stop signal received in exportClusterHandler.")
			return
		}
	}
}

// SendDeployAdd Function
func (exp *ExpHandler) SendDeployAdd(dep *protobuf.Deploy) error {
	exp.exporterLock.Lock()
	defer exp.exporterLock.Unlock()

	failed := 0
	total := len(exp.deployAddExporters)

	newList := make([]*deployAddStreamInform, 0, total)

	for _, dsi := range exp.deployAddExporters {
		if err := dsi.stream.Send(dep); err != nil {
			failed++
			log.Printf("[Exporter] Failed to send AddDeployEvent to %s (%s): %v",
				dsi.Hostname, dsi.IPAddress, err)
		} else {
			newList = append(newList, dsi)
		}
	}

	exp.deployAddExporters = newList

	if failed > 0 {
		return fmt.Errorf("SendDeployAdd failed: %d/%d", failed, total)
	}
	return nil
}

func (exp *ExpHandler) SendDeployUpdate(dep *protobuf.Deploy) error {
	exp.exporterLock.Lock()
	defer exp.exporterLock.Unlock()

	failed := 0
	total := len(exp.deployUpdateExporters)

	newList := make([]*deployUpdateStreamInform, 0, total)

	for _, dsi := range exp.deployUpdateExporters {
		if err := dsi.stream.Send(dep); err != nil {
			failed++
			log.Printf("[Exporter] Failed to send UpdateDeployEvent to %s (%s): %v",
				dsi.Hostname, dsi.IPAddress, err)
		} else {
			newList = append(newList, dsi)
		}
	}

	exp.deployUpdateExporters = newList

	if failed > 0 {
		return fmt.Errorf("SendDeployUpdate failed: %d/%d", failed, total)
	}
	return nil
}

// SendDeployDelete Function
func (exp *ExpHandler) SendDeployDelete(dep *protobuf.Deploy) error {
	exp.exporterLock.Lock()
	defer exp.exporterLock.Unlock()

	failed := 0
	total := len(exp.deployDeleteExporters)

	newList := make([]*deployDeleteStreamInform, 0, total)

	for _, dsi := range exp.deployDeleteExporters {
		if err := dsi.stream.Send(dep); err != nil {
			failed++
			log.Printf("[Exporter] Failed to send DeleteDeployEvent to %s (%s): %v",
				dsi.Hostname, dsi.IPAddress, err)
		} else {
			newList = append(newList, dsi)
		}
	}

	exp.deployDeleteExporters = newList

	if failed > 0 {
		return fmt.Errorf("SendDeployDelete failed: %d/%d", failed, total)
	}
	return nil
}

// AddDeployEventDB Function
func (exs *ExpService) AddDeployEventDB(info *protobuf.ClientInfo, stream protobuf.KubeCarto_AddDeployEventDBServer) error {
	log.Printf("[Exporter] Client %s (%s) connected to AddDeployEventDB", info.HostName, info.IPAddress)

	dsi := &deployAddStreamInform{
		Hostname:  info.HostName,
		IPAddress: info.IPAddress,
		stream:    stream,
		errChan:   make(chan error),
	}

	ExpH.exporterLock.Lock()
	ExpH.deployAddExporters = append(ExpH.deployAddExporters, dsi)
	ExpH.exporterLock.Unlock()

	return <-dsi.errChan
}

// UpdateDeployEventDB Function
func (exs *ExpService) UpdateDeployEventDB(info *protobuf.ClientInfo, stream protobuf.KubeCarto_UpdateDeployEventDBServer) error {
	log.Printf("[Exporter] Client %s (%s) connected to UpdateDeployEventDB", info.HostName, info.IPAddress)

	dsi := &deployUpdateStreamInform{
		Hostname:  info.HostName,
		IPAddress: info.IPAddress,
		stream:    stream,
		errChan:   make(chan error),
	}

	ExpH.exporterLock.Lock()
	ExpH.deployUpdateExporters = append(ExpH.deployUpdateExporters, dsi)
	ExpH.exporterLock.Unlock()

	return <-dsi.errChan
}

// DeleteDeployEventDB Function
func (exs *ExpService) DeleteDeployEventDB(info *protobuf.ClientInfo, stream protobuf.KubeCarto_DeleteDeployEventDBServer) error {
	log.Printf("[Exporter] Client %s (%s) connected to DeleteDeployEventDB", info.HostName, info.IPAddress)

	dsi := &deployDeleteStreamInform{
		Hostname:  info.HostName,
		IPAddress: info.IPAddress,
		stream:    stream,
		errChan:   make(chan error),
	}

	ExpH.exporterLock.Lock()
	ExpH.deployDeleteExporters = append(ExpH.deployDeleteExporters, dsi)
	ExpH.exporterLock.Unlock()

	return <-dsi.errChan
}

// SendPodAdd Function
func (exp *ExpHandler) SendPodAdd(pod *protobuf.Pod) error {
	exp.exporterLock.Lock()
	defer exp.exporterLock.Unlock()

	failed := 0
	total := len(exp.podAddExporters)

	newList := make([]*podAddStreamInform, 0, total)
	for _, psi := range exp.podAddExporters {
		if err := psi.stream.Send(pod); err != nil {
			failed++
			log.Printf("[Exporter] Failed to send AddPodEvent to %s (%s): %v",
				psi.Hostname, psi.IPAddress, err)
		} else {
			newList = append(newList, psi)
		}
	}
	exp.podAddExporters = newList

	if failed > 0 {
		return fmt.Errorf("SendPodAdd failed: %d/%d", failed, total)
	}
	return nil
}

// SendPodUpdate Function
func (exp *ExpHandler) SendPodUpdate(pod *protobuf.Pod) error {
	exp.exporterLock.Lock()
	defer exp.exporterLock.Unlock()

	failed := 0
	total := len(exp.podUpdateExporters)

	newList := make([]*podUpdateStreamInform, 0, total)
	for _, psi := range exp.podUpdateExporters {
		if err := psi.stream.Send(pod); err != nil {
			failed++
			log.Printf("[Exporter] Failed to send UpdatePodEvent to %s (%s): %v",
				psi.Hostname, psi.IPAddress, err)
		} else {
			newList = append(newList, psi)
		}
	}
	exp.podUpdateExporters = newList

	if failed > 0 {
		return fmt.Errorf("SendPodUpdate failed: %d/%d", failed, total)
	}
	return nil
}

// SendPodDelete Function
func (exp *ExpHandler) SendPodDelete(pod *protobuf.Pod) error {
	exp.exporterLock.Lock()
	defer exp.exporterLock.Unlock()

	failed := 0
	total := len(exp.podDeleteExporters)

	newList := make([]*podDeleteStreamInform, 0, total)
	for _, psi := range exp.podDeleteExporters {
		if err := psi.stream.Send(pod); err != nil {
			failed++
			log.Printf("[Exporter] Failed to send DeletePodEvent to %s (%s): %v",
				psi.Hostname, psi.IPAddress, err)
		} else {
			newList = append(newList, psi)
		}
	}
	exp.podDeleteExporters = newList

	if failed > 0 {
		return fmt.Errorf("SendPodDelete failed: %d/%d", failed, total)
	}
	return nil
}

// AddPodEventDB Function
func (exs *ExpService) AddPodEventDB(info *protobuf.ClientInfo, stream protobuf.KubeCarto_AddPodEventDBServer) error {
	log.Printf("[Exporter] Client %s (%s) connected to AddPodEventDB", info.HostName, info.IPAddress)

	psi := &podAddStreamInform{
		Hostname:  info.HostName,
		IPAddress: info.IPAddress,
		stream:    stream,
		errChan:   make(chan error),
	}

	ExpH.exporterLock.Lock()
	ExpH.podAddExporters = append(ExpH.podAddExporters, psi)
	ExpH.exporterLock.Unlock()

	return <-psi.errChan
}

// UpdatePodEventDB Function
func (exs *ExpService) UpdatePodEventDB(info *protobuf.ClientInfo, stream protobuf.KubeCarto_UpdatePodEventDBServer) error {
	log.Printf("[Exporter] Client %s (%s) connected to UpdatePodEventDB", info.HostName, info.IPAddress)

	psi := &podUpdateStreamInform{
		Hostname:  info.HostName,
		IPAddress: info.IPAddress,
		stream:    stream,
		errChan:   make(chan error),
	}

	ExpH.exporterLock.Lock()
	ExpH.podUpdateExporters = append(ExpH.podUpdateExporters, psi)
	ExpH.exporterLock.Unlock()

	return <-psi.errChan
}

// DeletePodEventDB Function
func (exs *ExpService) DeletePodEventDB(info *protobuf.ClientInfo, stream protobuf.KubeCarto_DeletePodEventDBServer) error {
	log.Printf("[Exporter] Client %s (%s) connected to DeletePodEventDB", info.HostName, info.IPAddress)

	psi := &podDeleteStreamInform{
		Hostname:  info.HostName,
		IPAddress: info.IPAddress,
		stream:    stream,
		errChan:   make(chan error),
	}

	ExpH.exporterLock.Lock()
	ExpH.podDeleteExporters = append(ExpH.podDeleteExporters, psi)
	ExpH.exporterLock.Unlock()

	return <-psi.errChan
}

// SendServiceAdd Function
func (exp *ExpHandler) SendServiceAdd(svc *protobuf.Service) error {
	exp.exporterLock.Lock()
	defer exp.exporterLock.Unlock()

	failed := 0
	total := len(exp.svcAddExporters)

	newList := make([]*svcAddStreamInform, 0, total)
	for _, ssi := range exp.svcAddExporters {
		if err := ssi.stream.Send(svc); err != nil {
			failed++
			log.Printf("[Exporter] Failed to send AddSvcEvent to %s (%s): %v",
				ssi.Hostname, ssi.IPAddress, err)
		} else {
			newList = append(newList, ssi)
		}
	}
	exp.svcAddExporters = newList

	if failed > 0 {
		return fmt.Errorf("SendServiceAdd failed: %d/%d", failed, total)
	}
	return nil
}

// SendServiceUpdate Function
func (exp *ExpHandler) SendServiceUpdate(svc *protobuf.Service) error {
	exp.exporterLock.Lock()
	defer exp.exporterLock.Unlock()

	failed := 0
	total := len(exp.svcUpdateExporters)

	newList := make([]*svcUpdateStreamInform, 0, total)
	for _, ssi := range exp.svcUpdateExporters {
		if err := ssi.stream.Send(svc); err != nil {
			failed++
			log.Printf("[Exporter] Failed to send UpdateSvcEvent to %s (%s): %v",
				ssi.Hostname, ssi.IPAddress, err)
		} else {
			newList = append(newList, ssi)
		}
	}
	exp.svcUpdateExporters = newList

	if failed > 0 {
		return fmt.Errorf("SendServiceUpdate failed: %d/%d", failed, total)
	}
	return nil
}

// SendServiceDelete Function
func (exp *ExpHandler) SendServiceDelete(svc *protobuf.Service) error {
	exp.exporterLock.Lock()
	defer exp.exporterLock.Unlock()

	failed := 0
	total := len(exp.svcDeleteExporters)

	newList := make([]*svcDeleteStreamInform, 0, total)
	for _, ssi := range exp.svcDeleteExporters {
		if err := ssi.stream.Send(svc); err != nil {
			failed++
			log.Printf("[Exporter] Failed to send DeleteSvcEvent to %s (%s): %v",
				ssi.Hostname, ssi.IPAddress, err)
		} else {
			newList = append(newList, ssi)
		}
	}
	exp.svcDeleteExporters = newList

	if failed > 0 {
		return fmt.Errorf("SendServiceDelete failed: %d/%d", failed, total)
	}
	return nil
}

// AddSvcEventDB Function
func (exs *ExpService) AddSvcEventDB(info *protobuf.ClientInfo, stream protobuf.KubeCarto_AddSvcEventDBServer) error {
	log.Printf("[Exporter] Client %s (%s) connected to AddSvcEventDB", info.HostName, info.IPAddress)

	ssi := &svcAddStreamInform{
		Hostname:  info.HostName,
		IPAddress: info.IPAddress,
		stream:    stream,
		errChan:   make(chan error),
	}

	ExpH.exporterLock.Lock()
	ExpH.svcAddExporters = append(ExpH.svcAddExporters, ssi)
	ExpH.exporterLock.Unlock()

	return <-ssi.errChan
}

// UpdateSvcEventDB Function
func (exs *ExpService) UpdateSvcEventDB(info *protobuf.ClientInfo, stream protobuf.KubeCarto_UpdateSvcEventDBServer) error {
	log.Printf("[Exporter] Client %s (%s) connected to UpdateSvcEventDB", info.HostName, info.IPAddress)

	ssi := &svcUpdateStreamInform{
		Hostname:  info.HostName,
		IPAddress: info.IPAddress,
		stream:    stream,
		errChan:   make(chan error),
	}

	ExpH.exporterLock.Lock()
	ExpH.svcUpdateExporters = append(ExpH.svcUpdateExporters, ssi)
	ExpH.exporterLock.Unlock()

	return <-ssi.errChan
}

// DeleteSvcEventDB Function
func (exs *ExpService) DeleteSvcEventDB(info *protobuf.ClientInfo, stream protobuf.KubeCarto_DeleteSvcEventDBServer) error {
	log.Printf("[Exporter] Client %s (%s) connected to DeleteSvcEventDB", info.HostName, info.IPAddress)

	ssi := &svcDeleteStreamInform{
		Hostname:  info.HostName,
		IPAddress: info.IPAddress,
		stream:    stream,
		errChan:   make(chan error),
	}

	ExpH.exporterLock.Lock()
	ExpH.svcDeleteExporters = append(ExpH.svcDeleteExporters, ssi)
	ExpH.exporterLock.Unlock()

	return <-ssi.errChan
}
