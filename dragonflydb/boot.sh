#!/bin/sh
echo "正在启动 DragonflyDB..."
mkdir -p data
docker compose up -d
echo "启动完成。"
