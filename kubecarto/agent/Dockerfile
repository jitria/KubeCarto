# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 BoanLab @ DKU

### Builder

FROM golang:1.24-alpine3.21 as builder

RUN apk --no-cache update
RUN apk add --no-cache git clang llvm make gcc protobuf musl-dev
RUN apk add --update alpine-sdk

RUN go install github.com/golang/protobuf/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

RUN mkdir /kubecarto

WORKDIR /kubecarto
COPY kubecarto/ .

RUN CGO_ENABLED=1 CC=gcc go build -o /bin/agent ./agent/main.go

### Make executable image

FROM alpine:3.17 as agent

COPY --from=builder /bin/agent /agent

CMD ["/agent"]
