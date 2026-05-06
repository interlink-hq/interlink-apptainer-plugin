FROM golang:1.24 AS build-stage

WORKDIR /app

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/apptainer-sidecar cmd/main.go

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install Apptainer and runtime dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    software-properties-common ca-certificates curl bash \
  && add-apt-repository -y ppa:apptainer/ppa \
  && apt-get update \
  && apt-get install -y --no-install-recommends apptainer \
  && update-ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /root

COPY --from=build-stage /app/bin/apptainer-sidecar /sidecar/apptainer-sidecar

ENV APPTAINERCONFIGPATH=/root/ApptainerConfig.yaml

ENTRYPOINT ["/bin/sh", "-c", "SHARED_FS=true /sidecar/apptainer-sidecar"]
