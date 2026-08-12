@echo off
chcp 65001 >nul
echo 正在启动 Kafka（单节点）...
if not exist "data" mkdir "data"
docker compose up -d
echo 启动完成。
