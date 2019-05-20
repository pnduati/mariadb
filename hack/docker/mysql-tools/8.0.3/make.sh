#!/bin/bash
set -xeou pipefail

GOPATH=$(go env GOPATH)
REPO_ROOT=$GOPATH/src/github.com/kubedb/mariadb

source "$REPO_ROOT/hack/libbuild/common/lib.sh"
source "$REPO_ROOT/hack/libbuild/common/kubedb_image.sh"

DOCKER_REGISTRY=${DOCKER_REGISTRY:-kubedb}

IMG=mysql-tools

DB_VERSION=8.0.3
TAG="$DB_VERSION"

OSM_VER=${OSM_VER:-0.9.1}

DIST=$REPO_ROOT/dist
mkdir -p $DIST

build() {
  pushd "$REPO_ROOT/hack/docker/mysql-tools/$DB_VERSION"

  # Download osm
  wget https://cdn.appscode.com/binaries/osm/${OSM_VER}/osm-alpine-amd64
  chmod +x osm-alpine-amd64
  mv osm-alpine-amd64 osm

  local cmd="docker build --pull -t $DOCKER_REGISTRY/$IMG:$TAG ."
  echo $cmd; $cmd

  rm osm
  popd
}

binary_repo $@
