#!/usr/bin/env python3
"""
validate_entry_list.py — 验证 entry-list-merged.json 格式

用法:
    python3 validate_entry_list.py entry-list-merged.json

退出码:
    0  — 通过
    1  — 格式错误（详见 stderr）
"""
import json
import re
import sys


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
            errors.append(f"{prefix} tag={tag!r} 不合法，必须是 'P' 或 'A'")

        # file
        file_ = item.get("file", "")
        if not isinstance(file_, str) or not file_.strip():
            errors.append(f"{prefix} file 为空或非字符串: {file_!r}")

        # line
        line = item.get("line")
        if not isinstance(line, int) or isinstance(line, bool):
            errors.append(f"{prefix} line={line!r} 类型错误，必须是整数")

        # function
        func = item.get("function", "")
        if not isinstance(func, str) or not func.strip():
            errors.append(f"{prefix} function 为空或非字符串: {func!r}")

        # taints
        taints = item.get("taints")
        if not isinstance(taints, list) or len(taints) == 0:
            errors.append(f"{prefix} taints={taints!r} 为空或非数组")
        elif any(not isinstance(t, str) or not t.strip() for t in taints):
            errors.append(f"{prefix} taints 含空字符串元素: {taints!r}")
        else:
            # 合法格式：标识符、param->member、param.member、Ns::name、source@field、@return
            # 不允许括号、空格、中文及其他特殊字符
            _TAINT = re.compile(
                r'^[a-zA-Z_@][a-zA-Z0-9_]*(?:(?:->|::|[.@])[a-zA-Z_][a-zA-Z0-9_]*)*$'
            )
            bad = [t for t in taints if not _TAINT.match(t)]
            if bad:
                errors.append(
                    f"{prefix} taints 含非法元素（只允许参数名/成员访问如"
                    f" param->field 或 @return，不能含括号/空格/中文）: {bad!r}"
                )

    return errors


def main():
    if len(sys.argv) < 2:
        print("用法: validate_entry_list.py <path>", file=sys.stderr)
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
