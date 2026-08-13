#!/bin/sh
echo "正在启动 PostgreSQL..."
mkdir -p data
docker compose up -d
echo "启动完成。"
