#!/bin/bash
set -xeou pipefail

GOPATH=$(go env GOPATH)
REPO_ROOT="$GOPATH/src/github.com/kubedb/mariadb"

export APPSCODE_ENV=prod

pushd $REPO_ROOT

rm -rf dist

./hack/docker/mariadb-operator/make.sh
./hack/docker/mariadb-operator/make.sh release

rm dist/.tag

popd
