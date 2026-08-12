#!/bin/sh
echo "正在启动 MySQL..."
mkdir -p data
docker compose up -d
echo "启动完成。"
