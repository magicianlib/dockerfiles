#!/bin/sh
echo "正在启动 Kafka 集群（3 节点）..."
mkdir -p data/kafka1
mkdir -p data/kafka2
mkdir -p data/kafka3
docker compose up -d
echo "启动完成。"
