from pathlib import Path
from typing import List, Tuple


def list_dir(path: Path) -> Tuple[List[Path], List[Path]]:
    dirs, files = [], []

    for p in sorted(path.iterdir()):
        if p.name.startswith("."):
            continue

        if p.is_dir():
            dirs.append(p)
        elif p.is_file():
            files.append(p)

    return dirs, files
