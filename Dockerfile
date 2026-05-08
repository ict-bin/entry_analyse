FROM public.ecr.aws/docker/library/ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# ═══ 系统工具 ═════════════════════════════════════════════════════════════════
RUN apt-get update && apt-get install -y \
    curl wget gnupg ca-certificates git zip \
    python3 python3-pip python3-venv \
    bubblewrap \
    && rm -rf /var/lib/apt/lists/*

# ═══ Node.js 22 ═══════════════════════════════════════════════════════════════
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# ═══ pi-coding-agent ══════════════════════════════════════════════════════════
RUN npm install -g @mariozechner/pi-coding-agent

# ═══ 项目代码 ═════════════════════════════════════════════════════════════════
WORKDIR /opt/entry_analyse
# 先拷贝依赖文件，使 pip install 层可被缓存（app/ 变更时不重装依赖）
COPY requirements.txt ./
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt -q
COPY app/               ./app/
COPY cli.py main.py     ./
COPY prompts/           ./prompts/
COPY scripts/           ./scripts/
COPY config.example.json .env.example ./
RUN find . -name '*.sh' -exec sed -i 's/\r$//' {} + && chmod +x scripts/*.sh 2>/dev/null || true

# ═══ pi 配置目录 ══════════════════════════════════════════════════════════════
ENV PI_CODING_AGENT_DIR=/root/.pi/agent
RUN mkdir -p /root/.pi/agent

# ═══ 挂载点 ═══════════════════════════════════════════════════════════════════
#
# /data/target  — 软件包目录（二进制+反汇编代码+模块分析文件，只读）
# /data/config  — config.json + models.json（只读）
# /data/output  — 输出目录
#
RUN mkdir -p /data/target /data/config /data/output /data/sessions

ENV PORT=3000
ENV OUTPUT_DIR=/data/output
ENV ARCHIVE_DIR=/data/output
ENV RESULT_DIR=/data/output

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# ═══ 入口脚本 ═════════════════════════════════════════════════════════════════
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# 默认 REST API，覆盖: python3 cli.py "分析xxx模块的外部入口"
CMD ["python3", "main.py"]
