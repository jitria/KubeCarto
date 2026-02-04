# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 BoanLab @ DKU

.PHONY: all build build-agent build-manager docker docker-agent docker-manager clean

MODULE_DIR := kubecarto

all: build

## Build binaries
build: build-agent build-manager

build-agent:
	cd $(MODULE_DIR) && go build -o ../bin/agent ./agent/main.go

build-manager:
	cd $(MODULE_DIR) && go build -o ../bin/manager ./manager/main.go

## Docker images
docker: docker-agent docker-manager

docker-agent:
	docker build --target agent -t boanlab/kubecarto-agent:v0.1 -f kubecarto/agent/Dockerfile .

docker-manager:
	docker build --target manager -t boanlab/kubecarto-manager:v0.1 -f kubecarto/manager/Dockerfile .

## Cleanup
clean:
	rm -rf bin/
	cd $(MODULE_DIR) && go clean
