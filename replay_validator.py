import os
import re

_SECURE_RE = re.compile(r"[^\w.-]")

MAX_CONTENT_LENGTH = 5 * 1024 * 1024


def secure_filename(filename: str) -> str:
    """Sanitize a filename, similar to werkzeug.utils.secure_filename."""
    name = os.path.basename(filename)
    name = _SECURE_RE.sub("_", name)
    name = name.lstrip(".")
    return name


def _validate_filename(filename: str) -> tuple[str, str | None]:
    if not filename or not filename.lower().endswith(".replay"):
        return "", "Only .replay files are accepted"
    safe_name = secure_filename(filename)
    if not safe_name.lower().endswith(".replay") or safe_name == ".replay":
        return "", "Invalid filename"
    return safe_name, None


def _validate_size(size: int) -> tuple[str | None, int]:
    if size > MAX_CONTENT_LENGTH:
        return "File too large (maximum 5MB)", 413
    return None, 200


def validate(filename: str, size: int) -> tuple[str, str | None, int]:
    """Validate a replay upload. Returns (safe_name, error, status_code)."""
    safe_name, error = _validate_filename(filename)
    if error:
        return "", error, 400
    error, status_code = _validate_size(size)
    if error:
        return "", error, status_code
    return safe_name, None, 200
