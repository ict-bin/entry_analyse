#!/bin/bash
# 容器入口脚本
# 确保 pi 配置目录存在，然后执行传入的 CMD

set -e

PI_DIR="${PI_CODING_AGENT_DIR:-/root/.pi/agent}"
mkdir -p "$PI_DIR"

if [ -d /data/config/prompts ]; then
    echo "[entrypoint] custom prompts found at /data/config/prompts/"
fi

exec "$@"
