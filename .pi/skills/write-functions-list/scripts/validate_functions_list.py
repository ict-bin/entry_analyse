#!/usr/bin/env python3
"""
validate_functions_list.py — 验证 functions.list 格式

用法:
    python3 validate_functions_list.py functions.list

退出码:
    0  — 通过
    1  — 格式错误（详见 stderr）
"""
import json
import re
import sys


# 与 app/functions_list.py 中 _TAINT_RE 保持一致
# 允许末尾 () —— 例如 aFrame->GetChannel()
_TAINT_RE = re.compile(
    r'^[a-zA-Z_@][a-zA-Z0-9_]*'
    r'(?:(?:->|::|[.@])[a-zA-Z_][a-zA-Z0-9_]*)*'
    r'(?:\(\))?$'
)


def validate(path: str) -> list[str]:
    errors: list[str] = []
    try:
        text = open(path, encoding="utf-8").read()
    except OSError as e:
        return [f"无法读取文件: {e}"]

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        return [f"JSON 解析失败: {e}"]

    if not isinstance(data, list):
        return [f"根类型必须是数组，实际是 {type(data).__name__}"]

    if len(data) == 0:
        errors.append("数组为空 — 应包含至少 1 个入口")
        return errors

    for i, item in enumerate(data):
        prefix = f"[{i}]"
        if not isinstance(item, dict):
            errors.append(f"{prefix} 元素类型错误: {type(item).__name__}")
            continue

        # _error 字段
        if "_error" in item:
            errors.append(f"{prefix} 含 _error 字段: {item['_error']}")
            continue

        # tag
        tag = item.get("tag")
        if tag not in ("P", "A"):
            errors.append(f"{prefix} tag={json.dumps(tag, ensure_ascii=False)} 不合法，必须是 'P' 或 'A'")

        # file
        file_ = item.get("file", "")
        if not isinstance(file_, str) or not file_.strip():
            errors.append(f"{prefix} file 为空或非字符串: {json.dumps(file_, ensure_ascii=False)}")

        # line
        line = item.get("line")
        if not isinstance(line, int) or isinstance(line, bool):
            errors.append(f"{prefix} line={json.dumps(line, ensure_ascii=False)} 类型错误，必须是整数")

        # function
        func = item.get("function", "")
        if not isinstance(func, str) or not func.strip():
            errors.append(f"{prefix} function 为空或非字符串: {json.dumps(func, ensure_ascii=False)}")

        # taints
        taints = item.get("taints")
        if not isinstance(taints, list) or len(taints) == 0:
            errors.append(f"{prefix} taints={json.dumps(taints, ensure_ascii=False)} 为空或非数组")
        elif any(not isinstance(t, str) or not t.strip() for t in taints):
            errors.append(f"{prefix} taints 含空字符串元素: {json.dumps(taints, ensure_ascii=False)}")
        else:
            bad = [t for t in taints if not _TAINT_RE.match(t)]
            if bad:
                errors.append(
                    f"{prefix} taints 含非法元素（只允许参数名/成员访问如"
                    f" param->field 或 @return，不能含括号（除末尾()）/空格/中文）:"
                    f" {json.dumps(bad, ensure_ascii=False)}"
                )

    return errors


def main():
    if len(sys.argv) < 2:
        print("用法: validate_functions_list.py <path>", file=sys.stderr)
        sys.exit(1)

    path = sys.argv[1]
    errors = validate(path)

    if errors:
        print(f"❌ {path}: {len(errors)} 个错误", file=sys.stderr)
        for e in errors:
            print(f"  • {e}", file=sys.stderr)
        sys.exit(1)

    try:
        count = len(json.loads(open(path, encoding="utf-8").read()))
    except Exception:
        count = 0
    print(f"✅ {path}: {count} entries, all fields valid")
    sys.exit(0)


if __name__ == "__main__":
    main()
