from typing import List
import time


def current_time() -> int:
    return round(time.time())


def sanitize_path(path: str) -> str:
    if not path.endswith("/"):
        path += "/"

    return path


def pretty_print(header: str, rows: List[str]) -> None:
    print("*" * 50)
    print(f"{header}")
    print("*" * 50)

    for it in rows:
        print(it)

    print("*" * 50)
    print("\n" * 2)
