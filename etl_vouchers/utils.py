from typing import List
import time


def current_time() -> int:
    """
    :return: int value - current microseconds.
    """
    return round(time.time())


def sanitize_path(path: str) -> str:
    """
    :return: path in correct format required by the system.
    """
    if not path.endswith("/"):
        path += "/"

    return path


def pretty_print(header: str, rows: List[str]) -> None:
    """
    Prints the output in subjectively beautiful format.
    :return: None
    """
    print("*" * 50)
    print(f"{header}")
    print("*" * 50)

    for it in rows:
        print(it)

    print("*" * 50)
    print("\n" * 2)
