@echo off
chcp 65001 >nul
echo 正在启动 PostgreSQL...
if not exist "data" mkdir "data"
docker compose up -d
echo 启动完成。
