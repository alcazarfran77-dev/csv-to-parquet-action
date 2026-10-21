"""
Entry point of the "CSV and JSON to Parquet" GitHub Action.

Reads the inputs from CTP_* environment variables (set by action.yml), converts
the files with the engine in src/, prints one line per file, adds error and
warning annotations, writes the step outputs and a job summary.
"""
from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import engine  # noqa: E402
import i18n  # noqa: E402
from i18n import t  # noqa: E402

# Texts of the action itself (the engine's messages come from src/locales)
TEXTS = {
    "en": {
        "bad_input": "Invalid value for input “{name}”: “{value}”. {hint}",
        "hint_bool": "Use true or false.",
        "hint_choice": "Use one of: {choices}.",
        "hint_number": "Use a number greater than 0.",
        "hint_split": "Use split-rows or split-mb, not both.",
        "no_path": "The “path” input is empty.",
        "no_match": "Nothing matches “{pattern}”.",
        "no_files": "No CSV or JSON files found.",
        "combined_exists": "The combined Parquet already exists. Set overwrite: true to replace it.",
        "failed": "{n} file(s) could not be converted.",
        "title": "CSV to Parquet",
        "line": "**{ok}** converted · **{errors}** failed · **{skipped}** skipped · {rows} · {sizes}",
        "sizes": "{src_size} of source files → {pq_size} of Parquet",
        "combined": "Combined into `{path}` ({size}{parts}).",
        "head": "| File | Status | Rows | Columns | Parquet | Notes |",
    },
    "es": {
        "bad_input": "Valor no válido para «{name}»: «{value}». {hint}",
        "hint_bool": "Usa true o false.",
        "hint_choice": "Usa uno de estos: {choices}.",
        "hint_number": "Usa un número mayor que 0.",
        "hint_split": "Usa split-rows o split-mb, no los dos.",
        "no_path": "La entrada «path» está vacía.",
        "no_match": "Nada coincide con «{pattern}».",
        "no_files": "No se encontraron archivos CSV ni JSON.",
        "combined_exists": "El Parquet unificado ya existe. Usa overwrite: true para reemplazarlo.",
        "failed": "{n} archivo(s) no se pudieron convertir.",
        "title": "CSV a Parquet",
        "line": "**{ok}** correctos · **{errors}** con error · **{skipped}** omitidos · {rows} · {sizes}",
        "sizes": "{src_size} originales → {pq_size} de Parquet",
        "combined": "Unificado en `{path}` ({size}{parts}).",
        "head": "| Archivo | Estado | Filas | Columnas | Parquet | Notas |",
    },
}


class InputError(Exception):
    pass


def text(key: str, **params) -> str:
    template = TEXTS[i18n.get_language()][key]
    return template.format(**{k: i18n._format_param(k, v) for k, v in params.items()})


# --------------------------------------------------------------------------- inputs
def raw(name: str) -> str:
    return os.environ.get("CTP_" + name.upper().replace("-", "_"), "").strip()


def flag(name: str, default: bool) -> bool:
    value = raw(name).lower()
    if not value:
        return default
    if value in ("true", "yes", "1", "on"):
        return True
    if value in ("false", "no", "0", "off"):
        return False
    raise InputError(text("bad_input", name=name, value=value, hint=text("hint_bool")))


def choice(name: str, choices: dict[str, str] | tuple[str, ...], default: str) -> str:
    value = raw(name) or default
    options = choices if isinstance(choices, dict) else {c: c for c in choices}
    key = value if value in options else value.lower()
    if key not in options:
        raise InputError(text("bad_input", name=name, value=value, hint=text("hint_choice", choices=", ".join(options))))
    return options[key]


def number(name: str, kind=float):
    value = raw(name)
    if not value:
        return None
    try:
        n = kind(value.replace(",", "") if kind is int else value)
    except ValueError:
        n = 0
    if n <= 0:
        raise InputError(text("bad_input", name=name, value=value, hint=text("hint_number")))
    return n


def read_options() -> engine.Options:
    separators = {"auto": None, ",": ",", ";": ";", "|": "|", "tab": "\t", "\\t": "\t", "\t": "\t",
                  "comma": ",", "semicolon": ";", "pipe": "|"}
    sep_value = os.environ.get("CTP_SEPARATOR", "")      # not stripped: it may be a tab
    sep_key = sep_value if sep_value == "\t" else (sep_value.strip() or "auto")
    if sep_key not in separators and sep_key.lower() not in separators:
        raise InputError(text("bad_input", name="separator", value=sep_key,
                              hint=text("hint_choice", choices='auto, ",", ";", "|", tab')))
    split_rows, split_mb = number("split-rows", int), number("split-mb", float)
    if split_rows and split_mb:
        raise InputError(text("hint_split"))
    combine = raw("combine")
    return engine.Options(
        separator=separators.get(sep_key, separators.get(sep_key.lower())),
        encoding=choice("encoding", engine.ENCODINGS, "auto"),
        decimal=choice("decimal", engine.DECIMALS, "auto"),
        header=flag("header", True),
        dmy_dates=flag("dmy-dates", True),
        all_text=flag("all-text", False),
        skip_bad_rows=flag("skip-bad-rows", False),
        compression=choice("compression", engine.COMPRESSIONS, "snappy"),
        recursive=flag("recursive", True),
        overwrite=flag("overwrite", False),
        keep_structure=flag("keep-structure", True),
        combine=bool(combine),
        combined_name=combine,
        source_column=flag("source-column", True),
        split_mode="rows" if split_rows else "mb" if split_mb else None,
        split_value=split_rows or split_mb or 0,
    )


# --------------------------------------------------------------------------- GitHub helpers
def escape_data(s: str) -> str:
    return s.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def escape_property(s: str) -> str:
    return escape_data(s).replace(":", "%3A").replace(",", "%2C")


def annotate(level: str, message: str, file: Path | None = None) -> None:
    where = f" file={escape_property(display(file))}" if file is not None else ""
    print(f"::{level}{where}::{escape_data(message)}", flush=True)


def display(path: Path) -> str:
    """Path relative to the workspace when possible, with forward slashes."""
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def write_outputs(values: dict[str, str]) -> None:
    target = os.environ.get("GITHUB_OUTPUT")
    if not target:
        for k, v in values.items():
            print(f"output {k}={v!r}")
        return
    with open(target, "a", encoding="utf-8", newline="\n") as f:
        for k, v in values.items():
            if "\n" in v:
                delimiter = f"CTP_{uuid.uuid4().hex}"
                f.write(f"{k}<<{delimiter}\n{v}\n{delimiter}\n")
            else:
                f.write(f"{k}={v}\n")


def write_summary(lines: list[str]) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if target:
        with open(target, "a", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(lines) + "\n")


def cell(s: str) -> str:
    return s.replace("|", "\\|").replace("\n", " ")


def parquet_files(target: Path, op: engine.Options) -> list[Path]:
    if not engine.split_of(op):
        return [target] if target.exists() else []
    folder = engine.parts_folder(target)
    return sorted(folder.glob(f"{engine.base_name(target)}_*.parquet")) if folder.is_dir() else []


# --------------------------------------------------------------------------- main
def main() -> int:
    i18n.set_language(raw("language").lower()[:2] or "en")
    try:
        op = read_options()
        fail_on_error = flag("fail-on-error", True)
    except InputError as e:
        annotate("error", str(e))
        return 1
    patterns = [p.strip() for p in os.environ.get("CTP_PATH", "").splitlines() if p.strip() and not p.strip().startswith("#")]
    if not patterns:
        annotate("error", text("no_path"))
        return 1
    out_dir = Path(raw("output")) if raw("output") else None

    jobs, unmatched = engine.collect_jobs(patterns, op.recursive)
    for pattern in unmatched:
        annotate("warning", text("no_match", pattern=pattern))
    if not jobs:
        annotate("error", text("no_files"))
        write_outputs({"converted": "0", "failed": "0", "skipped": "0", "rows": "0", "files": ""})
        return 1

    combined: list[int] = []

    def notify(event, *data):
        if event == "done":
            r: engine.Result = data[1]
            status = t(f"status_{r.status}").upper()
            parts = " " + t("cli_in_parts", n=r.parts) if r.parts else ""
            print(f"[{status:9}] {display(r.source)}  ->  {t('rows_count', n=r.rows)}{parts}", flush=True)
            if r.status == "error":
                annotate("error", r.message, r.source)
            elif r.messages and r.status in ("ok", "skipped"):
                annotate("warning" if r.status == "ok" else "notice", r.message, r.source)
        elif event == "log":
            print(str(data[0]), flush=True)
        elif event == "combined":
            combined[:] = [data[0], data[1]]

    results = engine.convert_batch(jobs, out_dir, op, notify)
    if op.combine and not results:
        annotate("error", text("combined_exists"))
        return 1

    ok = [r for r in results if r.status == "ok"]
    errors = [r for r in results if r.status == "error"]
    skipped = [r for r in results if r.status == "skipped"]
    rows = sum(r.rows for r in ok)
    files: list[Path] = []
    if op.combine:
        if combined:
            files = parquet_files(engine.combined_target(jobs, out_dir, op), op)
    else:
        for r in ok:
            files += parquet_files(r.target, op)

    write_outputs({
        "converted": str(len(ok)), "failed": str(len(errors)), "skipped": str(len(skipped)),
        "rows": str(rows), "files": "\n".join(display(f) for f in files),
    })

    src_bytes = sum(r.source_bytes for r in ok)
    pq_bytes = combined[0] if combined else sum(r.parquet_bytes for r in ok)
    summary = [f"### {text('title')}", "",
               text("line", ok=len(ok), errors=len(errors), skipped=len(skipped), rows=t("rows_count", n=rows),
                    sizes=text("sizes", src_size=src_bytes, pq_size=pq_bytes)), ""]
    if op.combine and combined:
        parts = f", {t('parts_count', n=combined[1])}" if combined[1] else ""
        summary += [text("combined", path=display(engine.combined_target(jobs, out_dir, op)), size=combined[0],
                         parts=parts), ""]
    summary += [text("head"), "|---|---|---:|---:|---|---|"]
    for r in results:
        if r.status == "ok" and not op.combine:
            pq = i18n.fmt_bytes(r.parquet_bytes) + (f" ({t('parts_count', n=r.parts)})" if r.parts else "")
        else:
            pq = ""
        summary.append(f"| {cell(display(r.source))} | {t(f'status_{r.status}')} | "
                       f"{i18n.fmt_int(r.rows) if r.status == 'ok' else ''} | {r.columns if r.status == 'ok' else ''} | "
                       f"{pq} | {cell(r.message)} |")
    write_summary(summary)

    if errors and fail_on_error:
        annotate("error", text("failed", n=len(errors)))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
