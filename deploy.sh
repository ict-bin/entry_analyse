#!/usr/bin/env bash
# deploy.sh — 一键同步代码到远程、构建镜像、清理残留
set -e

SSH_OPTS="-i $HOME/.ssh/id_yyf_188"
REMOTE="icsl@172.31.23.188"
DIR="~/yyf/entry_analyse"
LOCAL="$(cd "$(dirname "$0")" && pwd)"

echo "=== 同步代码 ==="
scp $SSH_OPTS "$LOCAL/cli.py" "$LOCAL/main.py" "$LOCAL/config.example.json" "$LOCAL/Dockerfile" "$REMOTE:$DIR/"
cd "$LOCAL" && tar cf - app prompts scripts | ssh $SSH_OPTS $REMOTE "cd $DIR && rm -rf app prompts scripts && tar xf -"

echo "=== 构建镜像 ==="
ssh $SSH_OPTS $REMOTE "cd $DIR && docker build -t entry_analyse ."

echo "=== 清理残留镜像 ==="
ssh $SSH_OPTS $REMOTE "docker image prune -f"

echo "=== 当前镜像 ==="
ssh $SSH_OPTS $REMOTE "docker images --format 'table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedSince}}' | grep -E 'REPO|entry'"

echo "=== 完成 ==="
