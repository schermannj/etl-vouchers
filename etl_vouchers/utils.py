import time


def current_time():
    return round(time.time())


def sanitize_path(path):
    if not path.endswith("/"):
        path += "/"

    return path
