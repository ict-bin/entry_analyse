ARG SECFLOW_PI_AGENT_RUNTIME_IMAGE=ghcr.io/runshine/secflow-base-pi-agent-runtime:20260602
FROM ${SECFLOW_PI_AGENT_RUNTIME_IMAGE}

ARG SECFLOW_BUILD_VERSION=""
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bubblewrap \
        sqlite3 \
        universal-ctags \
    && rm -rf /var/lib/apt/lists/*

# ═══ 项目代码 ═════════════════════════════════════════════════════════════════
WORKDIR /opt/entry_analyse
# 先拷贝依赖文件，使 pip install 层可被缓存（app/ 变更时不重装依赖）
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -q
COPY app/               ./app/
COPY cli.py main.py     ./
COPY prompts/           ./prompts/
COPY scripts/           ./scripts/
COPY .pi/               ./.pi/
COPY config.example.json .env.example ./
RUN printf '{"build_version":"%s"}\n' "$SECFLOW_BUILD_VERSION" > /opt/entry_analyse/build_meta.json
RUN find . -name '*.sh' -exec sed -i 's/\r$//' {} + && chmod +x scripts/*.sh 2>/dev/null || true \
    && chmod +x .pi/skills/write-entry-list-json/scripts/validate_entry_list.py 2>/dev/null || true

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
    CMD curl -f http://localhost:18080/healthz || exit 1

# ═══ 入口脚本 ═════════════════════════════════════════════════════════════════
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# 默认 REST API，覆盖: python3 cli.py "分析xxx模块的外部入口"
CMD ["./scripts/start-with-probe.sh", "python3", "main.py"]
