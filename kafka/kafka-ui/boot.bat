@echo off
chcp 65001 >nul
echo 正在启动 Kafbat UI...
if not exist "data" mkdir "data"
docker compose up -d
echo 启动完成。
echo 管理界面: http://localhost:8080
echo 启动后在界面中添加集群；地址须为本容器可达的 Kafka 接入地址（连宿主机 Kafka 用 host.docker.internal:9092）。
