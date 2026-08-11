@echo off
if not exist "data" mkdir "data"
docker compose up -d
