@echo off
chcp 65001 >nul
echo 正在启动 Kafbat UI...
if not exist "data" mkdir "data"
docker compose up -d
echo 启动完成。
echo 管理界面: http://localhost:8080
echo 接入宿主机 Kafka：界面里填 kafka:19092（需与 .env 中 KAFKA_NETWORK 指向的 compose 网络对应）。
