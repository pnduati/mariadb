#!/bin/bash
set -xeou pipefail

DOCKER_REGISTRY=${DOCKER_REGISTRY:-kubedb}
IMG_REGISTRY=prom
IMG=mariadbd-exporter
TAG=v0.11.0
# Available image tags: https://hub.docker.com/r/prom/mariadbd-exporter/tags/

docker pull $IMG_REGISTRY/$IMG:$TAG

docker tag $IMG_REGISTRY/$IMG:$TAG "$DOCKER_REGISTRY/$IMG:$TAG"
docker push "$DOCKER_REGISTRY/$IMG:$TAG"
