#!/bin/sh
echo "正在启动 Kafka 与 Debezium Connect..."
mkdir -p data/kafka
mkdir -p data/debezium
docker compose up -d
echo "启动完成。"
