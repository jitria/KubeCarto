# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 BoanLab @ DKU

### Builder

FROM golang:1.24-alpine3.21 as builder

RUN apk --no-cache update
RUN apk add --no-cache git clang llvm make gcc protobuf musl-dev

RUN mkdir -p /kubecarto/protobuf
RUN mkdir /client

WORKDIR /kubecarto/protobuf
COPY kubecarto/protobuf/ .

WORKDIR /client
COPY clients/log-client .

RUN go build -o log-client

### Make executable image

FROM alpine:3.17 as client

COPY --from=builder /client/log-client /

CMD ["/log-client"]
