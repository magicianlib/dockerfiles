#!/bin/sh
echo "正在启动 Kafbat UI..."
mkdir -p data
docker compose up -d
echo "启动完成。"
echo "管理界面: http://localhost:8080"
echo "接入宿主机 Kafka：界面里填 kafka:19092（需与 .env 中 KAFKA_NETWORK 指向的 compose 网络对应）。"
