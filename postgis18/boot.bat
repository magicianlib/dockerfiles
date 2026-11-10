@echo off
chcp 65001 >nul
rem 用法: boot.bat [-r]（-r 为删除容器与数据后重新启动）
if "%~1"=="-r" goto reset
if not "%~1"=="" goto usage
goto start

:reset
set "confirm="
set /p "confirm=将停止容器并删除 data 目录，数据不可恢复，确定继续？(y/N) "
if /i not "%confirm%"=="y" (
    echo 已取消。
    exit /b 1
)
docker compose down
if exist "data" rmdir /s /q data
rem 宿主机删除大量文件后，Docker Desktop/WSL 的文件共享同步有延迟，
rem 立即启动可能读到目录残影导致初始化失败，等待片刻再继续
timeout /t 3 /nobreak >nul
goto start

:usage
echo 用法: boot.bat [-r]（-r 为删除容器与数据后重新启动）
exit /b 1

:start
echo 正在启动 PostgreSQL...
if not exist "data" mkdir "data"
docker compose up -d
echo 启动完成。
