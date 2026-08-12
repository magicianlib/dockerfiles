@echo off
chcp 65001 >nul
echo 正在启动 Kafka 与 Debezium Connect...
if not exist "data\kafka" mkdir "data\kafka"
if not exist "data\debezium" mkdir "data\debezium"
docker compose up -d
echo 启动完成。
