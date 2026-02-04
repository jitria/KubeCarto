// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package collector

import (
	"google.golang.org/grpc"
)

// collectorInterface Interface
type collectorInterface interface {
	registerService(server *grpc.Server)
}
