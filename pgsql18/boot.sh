#!/bin/sh
# 用法: ./boot.sh [-r]（-r 为删除容器与数据后重新启动）
if [ "$1" = "-r" ]; then
    printf "将停止容器并删除 data 目录，数据不可恢复，确定继续？(y/N) "
    read answer
    if [ "$answer" != "y" ] && [ "$answer" != "Y" ]; then
        echo "已取消。"
        exit 1
    fi
    docker compose down
    rm -rf data
    # 宿主机删除大量文件后，Docker Desktop/WSL 的文件共享同步有延迟，
    # 立即启动可能读到目录残影导致初始化失败，等待片刻再继续
    sleep 3
elif [ -n "$1" ]; then
    echo "用法: $0 [-r]（-r 为删除容器与数据后重新启动）"
    exit 1
fi
echo "正在启动 PostgreSQL..."
mkdir -p data
docker compose up -d
echo "启动完成。"
