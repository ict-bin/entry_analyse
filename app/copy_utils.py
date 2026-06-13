from __future__ import annotations

import os
import shutil
from typing import Union

PathLike = Union[str, os.PathLike[str]]


def _same_path_fast(src: PathLike, dst: PathLike) -> bool:
    return os.fspath(src) == os.fspath(dst)


def _same_path_real(src: PathLike, dst: PathLike) -> bool:
    return os.path.realpath(os.fspath(src)) == os.path.realpath(os.fspath(dst))


def safe_copy2(src: PathLike, dst: PathLike, *, follow_symlinks: bool = True) -> str:
    if _same_path_fast(src, dst) or _same_path_real(src, dst):
        return "reused"
    shutil.copy2(src, dst, follow_symlinks=follow_symlinks)
    return "copied"


def safe_copyfile(src: PathLike, dst: PathLike, *, follow_symlinks: bool = True) -> str:
    if _same_path_fast(src, dst) or _same_path_real(src, dst):
        return "reused"
    shutil.copyfile(src, dst, follow_symlinks=follow_symlinks)
    return "copied"
