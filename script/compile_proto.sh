#!/bin/bash
# 在chord路径底下执行该脚本
PROTO_DIRS=( \
            "models/bridge" \
            "models/chord" \
           )

for dir in "${PROTO_DIRS[@]}"
do
    protoc --go_out="$dir" --go_opt=paths=source_relative \
           --go-grpc_out="$dir" --go-grpc_opt=paths=source_relative \
           -I ./"$dir" \
           "$dir"/*.proto
done
