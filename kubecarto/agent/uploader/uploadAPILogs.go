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

// UploadAPILog Function
func UploadAPILog(apiLog *protobuf.APILog) {
	UplH.uploaderAPILogs <- apiLog
}

// uploadAPILogs Function
func (upl *UplHandler) uploadAPILogs(wg *sync.WaitGroup) {
	wg.Add(1)

	for {
		select {
		case apiLog, ok := <-upl.uploaderAPILogs:
			if !ok {
				log.Printf("[Uploader] Failed to fetch APIs from APIs channel")
				wg.Done()
				return
			}

			if err := upl.sendAPILogs(apiLog); err != nil {
				log.Printf("[Uploader] Failed to upload API Logs: %v", err)
			}

		case <-upl.stopChan:
			wg.Done()
			return
		}
	}
}

// sendAPILogs Function
func (upl *UplHandler) sendAPILogs(apiLog *protobuf.APILog) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := upl.grpcClient.GiveAPILog(ctx)
	if err != nil {
		return fmt.Errorf("failed to open GiveAPILog stream: %w", err)
	}

	if err := stream.Send(apiLog); err != nil {
		return fmt.Errorf("failed to send APILog: %w", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close GiveAPILog stream: %w", err)
	}
	log.Printf("[Uploader] API Log sent, response: %v", resp)
	return nil
}
