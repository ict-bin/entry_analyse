#!/bin/bash
# 容器入口脚本
# 确保 pi 配置目录存在，然后执行传入的 CMD

set -e

PI_DIR="${PI_CODING_AGENT_DIR:-/root/.pi/agent}"
mkdir -p "$PI_DIR"

# 大任务会高频打开 SQLite WAL 文件、session 文件和 pi 管道；默认 1024 容易触发 EMFILE。
# 如果运行时权限允许，将 nofile 提升到 65535；失败时只告警，不阻塞启动。
FD_LIMIT="${EA_NOFILE_LIMIT:-65535}"
if ulimit -n "$FD_LIMIT" 2>/dev/null; then
    echo "[entrypoint] nofile limit set to $(ulimit -n)"
else
    echo "[entrypoint] warn: failed to set nofile limit to ${FD_LIMIT}; current=$(ulimit -n)"
fi

if [ -d /data/config/prompts ]; then
    echo "[entrypoint] custom prompts found at /data/config/prompts/"
fi

exec "$@"
