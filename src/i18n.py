"""
Translations for the desktop app.

The strings live in ../locales/<language>.json and are shared with the browser
version, so both apps always say the same thing. A string can be plain text or
a {"one": ..., "other": ...} object for singular/plural; placeholders look like
{name}. Parameters are formatted for the current language:

  * integers get thousands separators (1,234 / 1.234),
  * parameters named ``size`` or ending in ``_size`` are byte counts (13.8 MB / 13,8 MB),
  * parameters named ``seconds`` are durations (4.2 s / 4,2 s).
"""
from __future__ import annotations

import json
import locale
import os
import re
import sys
from pathlib import Path

LANGUAGES = {"en": "English", "es": "Español"}
DEFAULT_LANGUAGE = "en"

_strings: dict[str, dict] = {}
_language = DEFAULT_LANGUAGE
_PLACEHOLDER = re.compile(r"\{(\w+)\}")


def _locales_dir() -> Path:
    here = Path(__file__).resolve().parent
    candidates = [here / "locales", here.parent / "locales"]
    bundle = getattr(sys, "_MEIPASS", None)          # PyInstaller executable
    if bundle:
        candidates.insert(0, Path(bundle) / "locales")
    for folder in candidates:
        if (folder / f"{DEFAULT_LANGUAGE}.json").is_file():
            return folder
    raise FileNotFoundError("Translation files not found (expected a 'locales' folder next to the app).")


def _load() -> None:
    if _strings:
        return
    folder = _locales_dir()
    for code in LANGUAGES:
        _strings[code] = json.loads((folder / f"{code}.json").read_text(encoding="utf-8"))


def strings(code: str) -> dict:
    """All strings of a language (used by the tests)."""
    _load()
    return _strings[code]


def detect_language() -> str:
    """Spanish if the operating system is in Spanish, English otherwise."""
    if sys.platform.startswith("win"):
        try:
            import ctypes

            lang_id = ctypes.windll.kernel32.GetUserDefaultUILanguage()
            return "es" if (lang_id & 0x3FF) == 0x0A else "en"
        except Exception:  # noqa: BLE001
            pass
    for var in ("LC_ALL", "LC_MESSAGES", "LANG", "LANGUAGE"):
        value = os.environ.get(var, "")
        if value and value not in ("C", "POSIX", "C.UTF-8"):
            return "es" if value.lower().startswith("es") else "en"
    try:
        current = locale.getlocale()[0] or ""
    except Exception:  # noqa: BLE001
        current = ""
    return "es" if current.lower().startswith(("es", "spanish")) else "en"


def set_language(code: str) -> None:
    global _language
    _language = code if code in LANGUAGES else DEFAULT_LANGUAGE


def get_language() -> str:
    return _language


# --------------------------------------------------------------------------- #
#  Number formatting
# --------------------------------------------------------------------------- #
def _separators() -> tuple[str, str]:
    """(thousands, decimal) for the current language."""
    return (".", ",") if _language == "es" else (",", ".")


def fmt_int(n: int) -> str:
    thousands, _ = _separators()
    return f"{int(n):,}".replace(",", thousands)


def fmt_decimal(x: float, digits: int = 1) -> str:
    _, decimal = _separators()
    return f"{x:.{digits}f}".replace(".", decimal)


def fmt_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{fmt_decimal(n)} {unit}"
        n /= 1024
    return f"{fmt_decimal(n)} TB"


def fmt_seconds(s: float) -> str:
    if s >= 90:
        return f"{int(s // 60)} min {int(s % 60):02d} s"
    return f"{fmt_decimal(s)} s"


def _format_param(name: str, value) -> str:
    if isinstance(value, bool):
        return str(value)
    if name == "size" or name.endswith("_size"):
        return fmt_bytes(value)
    if name == "seconds":
        return fmt_seconds(value)
    if isinstance(value, int):
        return fmt_int(value)
    return str(value)


def t(key: str, /, **params) -> str:
    """Translated text for ``key`` in the current language."""
    _load()
    text = _strings[_language].get(key)
    if text is None:
        text = _strings[DEFAULT_LANGUAGE].get(key, key)
    if isinstance(text, dict):
        n = params.get("n", params.get("count", 0))
        text = text["one"] if n == 1 else text["other"]
    if not params:
        return text
    values = {k: _format_param(k, v) for k, v in params.items()}
    return _PLACEHOLDER.sub(lambda m: values.get(m.group(1), m.group(0)), text)
