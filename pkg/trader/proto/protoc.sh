#!/usr/bin/bash

export GOPATH=$HOME/go
export PATH=$PATH:$GOPATH/bin

protoc --go_out=../gen --go_opt=paths=source_relative \
    --go-grpc_out=../gen --go-grpc_opt=paths=source_relative \
    ./trader.proto

protoc --go_out=../gen --go_opt=paths=source_relative \
    --go-grpc_out=../gen --go-grpc_opt=paths=source_relative \
    ./resource-channel.proto

