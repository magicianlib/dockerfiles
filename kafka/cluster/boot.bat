@echo off
chcp 65001 >nul
echo 正在启动 Kafka 集群（3 节点）...
if not exist "data\kafka1" mkdir "data\kafka1"
if not exist "data\kafka2" mkdir "data\kafka2"
if not exist "data\kafka3" mkdir "data\kafka3"
docker compose up -d
echo 启动完成。
