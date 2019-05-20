#!/usr/bin/env bash

pushd $GOPATH/src/github.com/kubedb/mariadb/hack/gendocs
go run main.go
popd
