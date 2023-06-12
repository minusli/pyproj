import hashlib


def md5(raw: str) -> str:
    return hashlib.md5(raw.encode()).hexdigest()
