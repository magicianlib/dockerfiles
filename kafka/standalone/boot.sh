#!/bin/sh
echo "正在启动 Kafka（单节点）..."
mkdir -p data
docker compose up -d
echo "启动完成。"
