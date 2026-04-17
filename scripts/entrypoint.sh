#!/bin/bash
# 容器入口脚本
# 1. 如果挂载了 models.json，链接到 pi 配置目录
# 2. 执行传入的 CMD

set -e

PI_DIR="${PI_CODING_AGENT_DIR:-/root/.pi/agent}"
mkdir -p "$PI_DIR"

# 自动链接 models.json
if [ -f /data/config/models.json ]; then
    ln -sf /data/config/models.json "$PI_DIR/models.json"
    echo "[entrypoint] models.json linked → $PI_DIR/models.json"
    python3 - <<'PY'
import json
from pathlib import Path

pi_dir = Path("/root/.pi/agent")
models_path = Path("/data/config/models.json")
settings_path = pi_dir / "settings.json"

data = json.loads(models_path.read_text(encoding="utf-8"))
providers = data.get("providers", {})
provider_name, provider = next(iter(providers.items()))
model_id = "auto"
for model in provider.get("models", []):
    if model.get("id"):
        model_id = model["id"]
        break

settings_path.write_text(
    json.dumps({"defaultProvider": provider_name, "defaultModel": model_id}, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
print(f"[entrypoint] settings.json generated → {settings_path}")
PY
fi

# 自动链接自定义 prompts（如果有）
if [ -d /data/config/prompts ]; then
    echo "[entrypoint] custom prompts found at /data/config/prompts/"
fi

exec "$@"
