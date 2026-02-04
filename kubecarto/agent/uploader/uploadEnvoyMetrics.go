// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package uploader

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"kubecarto/protobuf"
)

// UploadEnvoyMetrics Function
func UploadEnvoyMetrics(evyMetrics *protobuf.EnvoyMetrics) {
	UplH.uploaderEnvoyMetrics <- evyMetrics
}

// uploadEnvoyMetrics Function
func (upl *UplHandler) uploadEnvoyMetrics(wg *sync.WaitGroup) {
	wg.Add(1)

	for {
		select {
		case evyMetrics, ok := <-upl.uploaderEnvoyMetrics:
			if !ok {
				log.Printf("[Uploader] Failed to fetch metrics from Envoy Metrics channel")
				wg.Done()
				return
			}

			if err := upl.sendEnvoyMetrics(evyMetrics); err != nil {
				log.Printf("[Uploader] Failed to upload Envoy metrics: %v", err)
			}

		case <-upl.stopChan:
			wg.Done()
			return
		}
	}
}

// SendEnvoyMetrics Function
func (upl *UplHandler) sendEnvoyMetrics(metrics *protobuf.EnvoyMetrics) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := upl.grpcClient.GiveEnvoyMetrics(ctx)
	if err != nil {
		return fmt.Errorf("failed to open GiveEnvoyMetrics stream: %w", err)
	}

	if err := stream.Send(metrics); err != nil {
		return fmt.Errorf("failed to send EnvoyMetrics: %w", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close GiveEnvoyMetrics stream: %w", err)
	}
	log.Printf("[Uploader] Envoy Metrics sent, response: %v", resp)
	return nil
}
