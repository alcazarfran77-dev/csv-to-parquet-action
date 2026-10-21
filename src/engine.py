"""
CSV / JSON -> Apache Parquet conversion engine.

Used by the desktop app (csv_to_parquet.py) and also usable on its own from
the command line:

    python engine.py <files, folders or patterns> [-o OUTPUT] [--split-rows N | --split-mb N]

Features:
  * Streaming: reads and writes in blocks, so files larger than RAM work.
  * CSV: detects separator, encoding (UTF-8 / Windows-1252) and decimal comma.
    If a column changes type halfway through the file, it is re-read as text.
  * JSON: a list of objects, JSON Lines (one object per line) or an object that
    wraps the list ({"data": [...]}). Nested objects are flattened
    ("customer.name") and inner lists are stored as JSON text.
  * Combine: merges several files into one Parquet, reconciling their schemas.
  * Split: writes each Parquet as parts of N rows or about N MB, inside a folder
    named after the file (sales/sales_0001.parquet, ...).
  * Writes to temporary files and only renames them when everything went
    well: a half-written Parquet is never left behind.

User-facing messages are returned as ``Msg`` objects (a translation key plus
parameters) so the interface can show them in the selected language.
"""
from __future__ import annotations

import csv
import datetime as dt
import gzip
import io
import json
import re
import shutil
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
import i18n  # noqa: E402

EXT_CSV = (".csv", ".tsv", ".csv.gz", ".tsv.gz")
EXT_JSON = (".json", ".jsonl", ".ndjson", ".json.gz", ".jsonl.gz", ".ndjson.gz")
EXTENSIONS = EXT_CSV + EXT_JSON
SOURCE_COLUMN = "source_file"
SAMPLE_SIZE = 1 << 20            # 1 MB to detect separator / encoding
INFERENCE_SIZE = 16 << 20        # 16 MB to infer the column types
BLOCK_SIZE = 4 << 20             # 4 MB per block (pyarrow reads up to 32 blocks ahead)
ROW_GROUP_BYTES = 64 << 20       # ~64 MB in memory per Parquet row group

SEPARATORS = ("auto", ",", ";", "\t", "|")
ENCODINGS = ("auto", "utf-8", "cp1252", "latin-1", "utf-16")
DECIMALS = ("auto", ".", ",")
COMPRESSIONS = ("snappy", "zstd", "gzip", "none")
SEPARATOR_KEYS = {",": "sep_name_comma", ";": "sep_name_semicolon", "\t": "sep_name_tab", "|": "sep_name_pipe"}
ENCODING_NAMES = {"utf-8": "UTF-8", "cp1252": "Windows-1252", "latin-1": "ISO-8859-1", "utf-16": "UTF-16"}


class Msg:
    """A message for the user: translation key + parameters, rendered on demand."""
    __slots__ = ("key", "params")

    def __init__(self, key: str, /, **params):
        self.key = key
        self.params = params

    def __str__(self) -> str:
        return i18n.t(self.key, **{k: (str(v) if isinstance(v, (Msg, MsgList)) else v) for k, v in self.params.items()})

    def __repr__(self) -> str:
        return f"Msg({self.key!r}, {self.params!r})"


class MsgList:
    """Several messages shown together, joined with ``separator``."""
    __slots__ = ("items", "separator")

    def __init__(self, items, separator: str = " "):
        self.items = list(items)
        self.separator = separator

    def __str__(self) -> str:
        return self.separator.join(str(m) for m in self.items)


class ConversionError(Exception):
    """An error explained to the user (see ``msg``)."""

    def __init__(self, key: str, /, **params):
        self.msg = Msg(key, **params)
        super().__init__(key)

    def __str__(self) -> str:
        return str(self.msg)


class Cancelled(Exception):
    """The user pressed Cancel."""


class _NotUTF8(Exception):
    """The file looked like UTF-8 but contains bytes that are not."""


@dataclass
class Options:
    separator: str | None = None        # None = detect
    encoding: str = "auto"
    header: bool = True
    decimal: str = "auto"               # auto | . | ,
    dmy_dates: bool = True
    all_text: bool = False
    skip_bad_rows: bool = False
    compression: str = "snappy"
    recursive: bool = True
    overwrite: bool = False
    combine: bool = False
    combined_name: str = "combined.parquet"
    source_column: bool = False
    keep_structure: bool = True
    split_mode: str | None = None       # None | "rows" | "mb"
    split_value: float = 0.0


@dataclass
class Result:
    source: Path
    target: Path | None = None
    status: str = "pending"             # ok | error | skipped | cancelled
    messages: list[Msg] = field(default_factory=list)
    rows: int = 0
    columns: int = 0
    source_bytes: int = 0
    parquet_bytes: int = 0
    seconds: float = 0.0
    separator: str = ""                 # separator code (CSV)
    encoding: str = ""
    decimal: str = "."
    skipped_rows: int = 0
    text_columns: list[str] = field(default_factory=list)
    parts: int = 0                      # > 0 when the output was split
    format: Msg | None = None

    @property
    def message(self) -> str:
        return " ".join(str(m) for m in self.messages)


@dataclass
class CsvFormat:
    """What was detected (or chosen) to read a CSV."""
    separator: str
    encoding: str
    names: list[str]
    decimal: str = "."


# --------------------------------------------------------------------------- #
#  Finding files
# --------------------------------------------------------------------------- #
def is_supported(path: Path) -> bool:
    return path.is_file() and path.name.lower().endswith(EXTENSIONS)


def is_json(path: Path) -> bool:
    return path.name.lower().endswith(EXT_JSON)


def list_files(paths: Iterable[Path], recursive: bool = True) -> list[tuple[Path, Path]]:
    """Returns [(file, root folder)] for every CSV or JSON file found."""
    seen: set[Path] = set()
    jobs: list[tuple[Path, Path]] = []
    for path in paths:
        path = Path(path)
        if path.is_dir():
            pattern = path.rglob("*") if recursive else path.glob("*")
            found = sorted((p for p in pattern if is_supported(p)), key=lambda p: str(p).lower())
            root = path
        elif is_supported(path):
            found, root = [path], path.parent
        else:
            continue
        for p in found:
            if p.resolve() not in seen:
                seen.add(p.resolve())
                jobs.append((p, root))
    return jobs


def base_name(path: Path) -> str:
    name = path.name
    for ext in sorted(EXTENSIONS, key=len, reverse=True):
        if name.lower().endswith(ext):
            return name[: -len(ext)]
    return path.stem


def target_path(file: Path, root: Path, out_dir: Path | None, keep_structure: bool) -> Path:
    name = base_name(file) + ".parquet"
    if out_dir is None:
        return file.parent / name
    if keep_structure:
        try:
            return out_dir / file.parent.relative_to(root) / name
        except ValueError:
            pass
    return out_dir / name


# --------------------------------------------------------------------------- #
#  CSV format detection
# --------------------------------------------------------------------------- #
def _open(path: Path):
    """Returns (stream, raw file). The raw file is used to measure progress."""
    raw = pa.OSFile(str(path), "rb")
    if path.name.lower().endswith(".gz"):
        return pa.CompressedInputStream(raw, "gzip"), raw
    return raw, raw


def read_sample(path: Path, n: int = SAMPLE_SIZE) -> bytes:
    stream, raw = _open(path)
    try:
        return stream.read(n)
    finally:
        stream.close()
        if stream is not raw:
            raw.close()


def detect_encoding(sample: bytes) -> str:
    if sample.startswith(b"\xef\xbb\xbf"):
        return "utf-8"
    if sample.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError as e:
        if e.start >= len(sample) - 3 and e.reason == "unexpected end of data":
            return "utf-8"  # the sample cut a character in half
    try:
        sample.decode("cp1252")
        return "cp1252"
    except UnicodeDecodeError:
        return "latin-1"


def detect_separator(text: str) -> str:
    lines = [line for line in text.splitlines() if line.strip()][:60]
    if len(lines) > 2:
        lines = lines[:-1]  # the last one may be cut
    best, best_score = ",", -1.0
    for sep in (",", ";", "\t", "|"):
        try:
            counts = [len(row) for row in csv.reader(lines, delimiter=sep)]
        except csv.Error:
            continue
        if not counts:
            continue
        mode = max(set(counts), key=counts.count)
        if mode < 2:
            continue
        score = (counts.count(mode) / len(counts)) * 1000 + mode
        if score > best_score:
            best, best_score = sep, score
    return best


_RE_COMMA = re.compile(r"^-?\d+,\d+$")
_RE_POINT = re.compile(r"^-?\d+\.\d+$")


def detect_decimal(text: str, sep: str) -> str:
    """Decimal comma only if the separator is not a comma and values like 12,5 dominate."""
    if sep == ",":
        return "."
    comma = point = 0
    for i, row in enumerate(csv.reader(io.StringIO(text), delimiter=sep)):
        if i == 0:
            continue
        if i > 1000:
            break
        for v in row:
            v = v.strip()
            if _RE_COMMA.match(v):
                comma += 1
            elif _RE_POINT.match(v):
                point += 1
    return "," if comma > point else "."


def clean_names(names: list[str]) -> list[str]:
    """Trims spaces, fills in empty names and avoids duplicates (case-insensitive)."""
    used: set[str] = set()
    out = []
    for i, n in enumerate(names):
        n = (n or "").replace("﻿", "").strip() or f"column_{i + 1}"
        base, k = n, 2
        while n.lower() in used:
            n, k = f"{base}_{k}", k + 1
        used.add(n.lower())
        out.append(n)
    return out


def detect_csv_format(path: Path, op: Options) -> CsvFormat:
    sample = read_sample(path)
    if not sample.strip():
        raise ConversionError("err_empty_file")
    enc = detect_encoding(sample) if op.encoding == "auto" else op.encoding
    text = sample.decode(enc, errors="replace").lstrip("﻿")
    sep = op.separator or detect_separator(text)
    first = next(csv.reader(io.StringIO(text), delimiter=sep), [])
    if op.header:
        names = clean_names(first)
    else:
        names = [f"column_{i + 1}" for i in range(len(first))]
    dec = detect_decimal(text, sep) if op.decimal == "auto" else op.decimal
    return CsvFormat(sep, enc, names, dec)


# --------------------------------------------------------------------------- #
#  Reading CSV
# --------------------------------------------------------------------------- #
def _pyarrow_options(fmt: CsvFormat, op: Options, types: dict, block: int, bad_rows: list):
    read = pacsv.ReadOptions(
        column_names=fmt.names,
        skip_rows=1 if op.header else 0,
        encoding=fmt.encoding,
        block_size=block,
    )

    def handle_bad_row(row):
        bad_rows.append(row)
        return "skip"

    parse = pacsv.ParseOptions(
        delimiter=fmt.separator,
        newlines_in_values=True,
        invalid_row_handler=handle_bad_row if op.skip_bad_rows else None,
    )
    if op.all_text:
        types = {n: pa.string() for n in fmt.names}
    parsers = [pacsv.ISO8601]
    if op.dmy_dates:
        parsers += ["%d/%m/%Y", "%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%d-%m-%Y"]
    convert = pacsv.ConvertOptions(
        column_types=types,
        decimal_point=fmt.decimal,
        timestamp_parsers=parsers,
    )
    return read, parse, convert


def separator_name(sep: str) -> Msg:
    """Name of a separator for the user ("comma", "semicolon"…)."""
    key = SEPARATOR_KEYS.get(sep)
    return Msg(key) if key else Msg("raw", text=repr(sep))


def error_message(e: Exception) -> Msg:
    """Explains an exception to the user."""
    if isinstance(e, ConversionError):
        return e.msg
    text = str(e)
    m = re.search(r"Expected (\d+) columns, got (\d+): (.*)", text, re.S)
    if m:
        row = m.group(3).strip().splitlines()[0][:120]
        return Msg("err_wrong_columns_norow", got=int(m.group(2)), expected=int(m.group(1)), text=row)
    if "invalid UTF8" in text:
        return Msg("err_not_utf8")
    if "Empty CSV" in text:
        return Msg("err_empty_file")
    if isinstance(e, PermissionError):
        return Msg("err_permission")
    return Msg("raw", text=text)


def preview(path: Path, op: Options, rows: int = 200):
    """Reads only the start of the file to show it on screen: (table, format)."""
    if is_json(path):
        return preview_json(path, op, rows)
    fmt = detect_csv_format(path, op)
    types: dict = {}
    for _ in range(len(fmt.names) + 1):
        stream, raw = _open(path)
        try:
            read, parse, convert = _pyarrow_options(fmt, op, types, 1 << 20, [])
            reader = pacsv.open_csv(stream, read, parse, convert)
            _check_utf8(reader.schema, fmt)
            batches, n = [], 0
            for batch in reader:
                batches.append(batch)
                n += batch.num_rows
                if n >= rows:
                    break
            table = pa.Table.from_batches(batches, schema=reader.schema)
            return table.slice(0, rows), fmt
        except (pa.ArrowInvalid, _NotUTF8) as e:
            if _is_utf8_error(e):
                if op.encoding == "auto" and fmt.encoding == "utf-8":
                    fmt.encoding = "cp1252"
                    continue
                raise ConversionError("err_not_utf8") from e
            col = _column_with_error(e, fmt)
            if col is None or col in types:
                msg = error_message(e)
                raise ConversionError(msg.key, **msg.params) from e
            types[col] = pa.string()
        finally:
            stream.close()
            if stream is not raw:
                raw.close()
    raise ConversionError("err_could_not_read")


def _check_utf8(schema: pa.Schema, fmt: CsvFormat) -> None:
    # pyarrow marks columns with bytes that are not valid UTF-8 as 'binary'
    if fmt.encoding == "utf-8" and any(pa.types.is_binary(f.type) for f in schema):
        raise _NotUTF8("invalid UTF8")


def _is_utf8_error(e: Exception) -> bool:
    return isinstance(e, _NotUTF8) or "invalid UTF8" in str(e)


def _column_with_error(e: Exception, fmt: CsvFormat) -> str | None:
    m = re.search(r"In CSV column #(\d+)", str(e))
    if m and "invalid UTF8" not in str(e):
        i = int(m.group(1))
        if i < len(fmt.names):
            return fmt.names[i]
    return None


def _output_schema(schema: pa.Schema) -> pa.Schema:
    """Completely empty columns are stored as text, not as type 'null'."""
    return pa.schema([pa.field(f.name, pa.string()) if pa.types.is_null(f.type) else f for f in schema])


# --------------------------------------------------------------------------- #
#  Output: one Parquet or, when splitting, several parts inside a folder named
#  after the file (sales/sales_0001.parquet, ...)
# --------------------------------------------------------------------------- #
def split_of(op: Options) -> tuple[str, float] | None:
    if op.split_mode in ("rows", "mb") and op.split_value and op.split_value > 0:
        return op.split_mode, float(op.split_value)
    return None


def _parquet_base(target: Path) -> str:
    n = target.name
    return n[:-8] if n.lower().endswith(".parquet") else n


def parts_folder(target: Path) -> Path:
    return target.parent / _parquet_base(target)


def _parts_pattern(base: str) -> re.Pattern:
    return re.compile(rf"^{re.escape(base)}_(\d{{4,}})\.parquet$", re.I)


def output_exists(target: Path, op: Options) -> bool:
    if not split_of(op):
        return target.exists()
    folder = parts_folder(target)
    if not folder.is_dir():
        return False
    pattern = _parts_pattern(_parquet_base(target))
    return any(pattern.match(p.name) for p in folder.iterdir())


class ParquetOutput:
    """
    Writes a Parquet file (or its parts) to temporary «.partial» files and only
    renames them in close(). abort() deletes everything from this run.
    """

    def __init__(self, target: Path, schema: pa.Schema, compression: str, split: tuple[str, float] | None):
        self.target = target
        self.schema = schema
        self.compression = compression
        self.split = split
        self.row_limit = max(1, int(split[1])) if split and split[0] == "rows" else 0
        self.byte_limit = max(0.01, split[1]) * 1048576 if split and split[0] == "mb" else 0
        self.parts: list[tuple[Path, Path, int, int]] = []   # (temporary, final, rows, bytes)
        self.writer = None
        self.sink = None
        self.temporary: Path | None = None
        self.final: Path | None = None
        self.part_rows = 0
        self.data_bytes = 0
        self.data_rows = 0

    def _part_path(self, k: int) -> Path:
        if not self.split:
            return self.target
        base = _parquet_base(self.target)
        return self.target.parent / base / f"{base}_{k:04d}.parquet"

    def _open(self):
        self.final = self._part_path(len(self.parts) + 1)
        self.final.parent.mkdir(parents=True, exist_ok=True)
        self.temporary = self.final.with_name(self.final.name + ".partial")
        self.sink = pa.OSFile(str(self.temporary), "wb")
        self.writer = pq.ParquetWriter(self.sink, self.schema, compression=self.compression)
        self.part_rows = 0

    def _close_part(self):
        self.writer.close()
        self.writer = None
        self.sink.close()
        self.sink = None
        self.parts.append((self.temporary, self.final, self.part_rows, self.temporary.stat().st_size))
        self.temporary = None

    def write(self, table: pa.Table):
        n, start = table.num_rows, 0
        while start < n:
            if self.writer is None:
                self._open()
            k = n - start
            if self.row_limit:
                k = min(k, self.row_limit - self.part_rows)
            if self.byte_limit:
                # small groups (≈ ¼ of the limit) so the requested size is not overshot much
                bpr = self.data_bytes / self.data_rows if self.data_rows else 0
                k = min(k, max(1, int(self.byte_limit / 4 / bpr)) if bpr else 10_000)
                if self.part_rows and bpr and self.sink.tell() + k * bpr > self.byte_limit:
                    self._close_part()
                    continue
            before = self.sink.tell()
            self.writer.write_table(table.slice(start, k))
            self.part_rows += k
            start += k
            self.data_bytes += self.sink.tell() - before
            self.data_rows += k
            if (self.row_limit and self.part_rows >= self.row_limit) or \
                    (self.byte_limit and self.sink.tell() >= self.byte_limit):
                self._close_part()

    def close(self) -> tuple[int, int]:
        """Finishes, renames the temporary files and returns (total bytes, number of parts)."""
        if self.writer is not None:
            self._close_part()
        if not self.parts:                       # no rows: a file with just the schema
            self._open()
            self._close_part()
        for temporary, final, _, _ in self.parts:
            temporary.replace(final)
        if self.split:
            self._delete_leftovers()
        return sum(p[3] for p in self.parts), len(self.parts)

    def _delete_leftovers(self):
        """When overwriting, removes parts of a previous conversion that no longer belong."""
        folder = parts_folder(self.target)
        pattern = _parts_pattern(_parquet_base(self.target))
        for p in folder.iterdir():
            m = pattern.match(p.name)
            if m and int(m.group(1)) > len(self.parts):
                p.unlink(missing_ok=True)

    def abort(self):
        try:
            if self.writer is not None:
                self.writer.close()
            if self.sink is not None:
                self.sink.close()
        except Exception:  # noqa: BLE001
            pass
        self.writer = self.sink = None
        for tmp in [self.temporary] + [p[0] for p in self.parts]:
            if tmp is not None:
                Path(tmp).unlink(missing_ok=True)
        self.parts = []


# --------------------------------------------------------------------------- #
#  Converting one file
# --------------------------------------------------------------------------- #
def convert_file(
    source: Path,
    target: Path,
    op: Options,
    progress: Callable[[float], None] | None = None,
    cancel: threading.Event | None = None,
    source_label: str | None = None,
    split: bool = True,
) -> Result:
    r = Result(source=source, target=target)
    start = time.perf_counter()
    division = split_of(op) if split else None
    try:
        r.source_bytes = source.stat().st_size
        if not op.overwrite and (output_exists(target, op) if division else target.exists()):
            r.status = "skipped"
            r.messages = [Msg("skipped_exists")]
            return r
        if is_json(source):
            _convert_json(source, target, op, r, progress, cancel, source_label, division)
            return r
        fmt = detect_csv_format(source, op)
        r.format = Msg("fmt_csv")
        r.separator = fmt.separator
        r.encoding, r.decimal = fmt.encoding, fmt.decimal
        types: dict = {}
        notes: list[Msg] = []
        while True:
            try:
                rows, bad, r.parquet_bytes, parts = _write_csv(source, target, fmt, op, types, progress,
                                                               cancel, source_label, division)
                r.parts = parts if division else 0
                break
            except (pa.ArrowInvalid, _NotUTF8) as e:
                if _is_utf8_error(e):
                    if op.encoding == "auto" and fmt.encoding == "utf-8":
                        fmt.encoding = r.encoding = "cp1252"   # retry as ANSI
                        notes.append(Msg("note_encoding_adjusted"))
                        continue
                    raise ConversionError("err_not_utf8") from e
                col = _column_with_error(e, fmt)
                if col is not None and col not in types:
                    types[col] = pa.string()          # retry with that column as text
                    r.text_columns.append(col)
                    continue
                raise
        r.rows, r.skipped_rows = rows, bad
        r.columns = len(fmt.names) + (1 if source_label else 0)
        r.status = "ok"
        if r.text_columns:
            notes.append(Msg("note_text_columns", columns=", ".join(r.text_columns)))
        if bad:
            notes.append(Msg("note_skipped_rows", n=bad))
        r.messages = notes
    except Cancelled:
        r.status, r.messages = "cancelled", [Msg("cancelled_by_user")]
    except Exception as e:  # noqa: BLE001 - explained to the user
        r.status, r.messages = "error", [error_message(e)]
    finally:
        r.seconds = time.perf_counter() - start
    return r


def _infer_types(path: Path, fmt: CsvFormat, op: Options) -> dict:
    """
    Infers the types from a large sample (16 MB) instead of just the first
    block. This way the streaming blocks can stay small (little RAM) without
    making type detection worse.
    """
    if op.all_text or fmt.encoding.startswith("utf-16"):
        return {}
    data = read_sample(path, INFERENCE_SIZE)
    if len(data) >= INFERENCE_SIZE:          # truncated sample: cut at the last line break
        cut = data.rfind(b"\n")
        if cut <= 0:
            return {}
        data = data[: cut + 1]
    read, parse, convert = _pyarrow_options(fmt, op, {}, INFERENCE_SIZE * 2, [])
    parse.invalid_row_handler = lambda _row: "skip"
    try:
        table = pacsv.read_csv(pa.BufferReader(data), read, parse, convert)
    except Exception:  # noqa: BLE001 - if the sample can't be read, types are inferred per block
        return {}
    return {f.name: f.type for f in table.schema if not pa.types.is_null(f.type) and not pa.types.is_binary(f.type)}


def _write_csv(source, target, fmt, op, types, progress, cancel, source_label, division):
    bad: list = []
    stream, raw = _open(source)
    size = max(source.stat().st_size, 1)
    writer = None
    rows = 0
    try:
        types = {**_infer_types(source, fmt, op), **types}     # columns forced to text win
        read, parse, convert = _pyarrow_options(fmt, op, types, BLOCK_SIZE, bad)
        reader = pacsv.open_csv(stream, read, parse, convert)
        _check_utf8(reader.schema, fmt)
        schema = _output_schema(reader.schema)
        if source_label is not None:
            schema = schema.append(pa.field(SOURCE_COLUMN, pa.string()))
        writer = ParquetOutput(target, schema, op.compression, division)
        pending, pending_bytes = [], 0

        def flush():
            nonlocal pending, pending_bytes
            if pending:
                writer.write(pa.Table.from_batches(pending, schema=schema))
                pending, pending_bytes = [], 0

        compressed, batches = source.name.lower().endswith(".gz"), 0
        for batch in reader:
            batches += 1
            if cancel is not None and cancel.is_set():
                raise Cancelled()
            table = pa.Table.from_batches([batch])
            if source_label is not None:
                table = table.append_column(SOURCE_COLUMN, pa.repeat(source_label, table.num_rows))
            table = table.cast(schema)
            pending.extend(table.to_batches())
            pending_bytes += table.nbytes
            rows += table.num_rows
            if pending_bytes >= ROW_GROUP_BYTES:
                flush()
            if progress:
                # pyarrow reads ahead, so progress is estimated from the blocks already processed
                done = raw.tell() if compressed else batches * BLOCK_SIZE
                progress(min(done / size, 0.99))
        flush()
        total, parts = writer.close()
        writer = None
        if progress:
            progress(1.0)
        return rows, len(bad), total, parts
    finally:
        if writer is not None:
            writer.abort()
        stream.close()
        if stream is not raw:
            raw.close()


# --------------------------------------------------------------------------- #
#  JSON
# --------------------------------------------------------------------------- #
@dataclass
class JsonFormat:
    """How a JSON file is organised."""
    mode: str                       # "lines" (JSON Lines) | "document"
    encoding: str
    key: str | None = None          # list of records inside an object: {"key": [...]}
    single: bool = False            # the file is a single object
    names: list[str] = field(default_factory=list)
    keys: list[str] = field(default_factory=list)

    def describe(self) -> Msg:
        if self.mode == "lines":
            return Msg("fmt_json_lines")
        if self.single:
            return Msg("fmt_json_single")
        if self.key is not None:
            return Msg("fmt_json_key", key=self.key)
        return Msg("fmt_json_list")


RECORD_LIMIT = 64 << 20             # a record (or a single-object JSON) may not exceed 64 MB
_DECODER = json.JSONDecoder()
_NON_SPACE = re.compile(r"\S")
_I64 = 2 ** 63
_SAFE = 2 ** 53 - 1
J_INT, J_FLOAT, J_BOOL, J_TEXT = 1, 2, 4, 8

_RE_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:[.,](\d{1,9}))?)?\s*(Z|[+-]\d{2}:?\d{2})?)?$")
_RE_DMY = re.compile(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2})(?:[.,](\d{1,3}))?)?)?$")
_EPOCH = dt.date(1970, 1, 1).toordinal()


def parse_date(v: str, dmy: bool):
    """Returns (milliseconds since 1970, has_time, has_zone) or None."""
    if not v or not ("0" <= v[0] <= "9"):
        return None
    m = _RE_ISO.match(v)
    zone = None
    if m:
        y, mo, d = int(m[1]), int(m[2]), int(m[3])
        has_time = m[4] is not None
        h, mi, se, frac, zone = (int(m[4]), int(m[5]), int(m[6] or 0), m[7] or "", m[8]) if has_time \
            else (0, 0, 0, "", None)
    elif dmy and (m := _RE_DMY.match(v)):
        d, mo, y = int(m[1]), int(m[2]), int(m[3])
        has_time = m[4] is not None
        h, mi, se, frac = (int(m[4]), int(m[5]), int(m[6] or 0), m[7] or "") if has_time else (0, 0, 0, "")
    else:
        return None
    try:
        days = dt.date(y, mo, d).toordinal() - _EPOCH
    except ValueError:
        return None
    if h > 23 or mi > 59 or se > 59:
        return None
    ms = days * 86400000 + ((h * 60 + mi) * 60 + se) * 1000 + (int((frac + "00")[:3]) if frac else 0)
    if zone and zone != "Z":
        zz = zone.replace(":", "")
        ms -= (-1 if zz[0] == "-" else 1) * (int(zz[1:3]) * 60 + int(zz[3:5])) * 60000
    return ms, has_time, bool(zone)


def _json_text(v) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def _flatten(obj: dict, prefix: str, out: list):
    """Appends (key, value, is_list) to out; nested objects are flattened with «.»."""
    for k, v in obj.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            _flatten(v, key, out)
        elif isinstance(v, list):
            out.append((key, _json_text(v), True))
        else:
            out.append((key, v, False))


def _json_type(seen: int, dates: bool, has_time: bool) -> str:
    if seen == J_INT:
        return "int"
    if seen in (J_FLOAT, J_INT | J_FLOAT):
        return "float"
    if seen == J_BOOL:
        return "bool"
    if seen == J_TEXT:
        return ("timestamp" if has_time else "date") if dates else "string"
    return "string"


class _JsonTypeGuesser:
    __slots__ = ("dmy", "seen", "valid", "literals", "dates", "has_time", "zone", "mixed")

    def __init__(self, dmy: bool):
        self.dmy = dmy
        self.seen = self.valid = self.literals = 0
        self.dates, self.has_time, self.zone, self.mixed = True, False, False, None

    def probe(self, v, is_list: bool, record: int):
        if v is None:
            return
        self.literals += 1
        kind = type(v)
        if kind is str and v == "":
            return                      # "" does not decide the type (in date columns it counts as empty)
        self.valid += 1
        if kind is bool:
            cat = J_BOOL
        elif kind is int:
            cat = J_INT if -_I64 <= v < _I64 else J_TEXT       # outside INT64: text
            if cat == J_TEXT:
                self.dates = False
        elif kind is float:
            # same as the browser version: 3.0 counts as an integer
            cat = J_INT if v.is_integer() and abs(v) <= _SAFE else J_FLOAT
        else:
            cat = J_TEXT
            if self.dates:
                d = None if is_list else parse_date(v, self.dmy)
                if d is None:
                    self.dates = False
                else:
                    self.has_time = self.has_time or d[1]
                    self.zone = self.zone or d[2]
        before = self.seen
        self.seen |= cat
        if (self.mixed is None and before and before != self.seen and _json_type(self.seen, False, False) == "string"
                and _json_type(before, self.dates, self.has_time) != "string"):
            self.mixed = (v if isinstance(v, str) else _json_text(v), record)

    def kind(self) -> str:
        if not self.valid:
            return "string" if self.literals else "null"
        return _json_type(self.seen, self.dates, self.has_time)


def _to_text(v) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and v.is_integer() and abs(v) <= _SAFE:
        return str(int(v))
    if isinstance(v, (int, float)):
        return repr(v)
    return _json_text(v)


def _json_converter(kind: str, dmy: bool):
    if kind == "int":
        return int
    if kind == "float":
        return float
    if kind == "bool":
        return lambda v: v

    if kind in ("date", "timestamp"):
        def to_date(v):
            if v == "":
                return None
            d = parse_date(v, dmy)
            if d is None:
                raise ValueError(v)
            return d[0] // 86400000 if kind == "date" else d[0]
        return to_date
    return _to_text


def _arrow_type(kind: str, utc: bool) -> pa.DataType:
    return {"int": pa.int64(), "float": pa.float64(), "bool": pa.bool_(), "date": pa.date32(),
            "timestamp": pa.timestamp("ms", tz="UTC" if utc else None)}.get(kind, pa.string())


def _looks_like_json_lines(text: str, complete: bool) -> bool:
    lines = [line for line in text.splitlines() if line.strip()]
    if not complete and lines:
        lines.pop()
    if len(lines) < 2:
        return False
    try:
        return all(isinstance(json.loads(line), dict) for line in lines[:5])
    except ValueError:
        return False


def detect_json_format(path: Path, op: Options) -> JsonFormat:
    sample = read_sample(path)
    if not sample.strip():
        raise ConversionError("err_empty_file")
    enc = detect_encoding(sample) if op.encoding == "auto" else op.encoding
    text = sample.decode(enc, errors="replace").lstrip("﻿").lstrip()
    name = path.name.lower().removesuffix(".gz")
    if name.endswith((".jsonl", ".ndjson")):
        mode = "lines"
    elif text.startswith("["):
        mode = "document"
    elif text.startswith("{"):
        mode = "lines" if _looks_like_json_lines(text, len(sample) < SAMPLE_SIZE) else "document"
    else:
        raise ConversionError("err_json_not_json")
    return JsonFormat(mode, enc)


def _open_text(path: Path, encoding: str, text: str | None = None):
    """Returns (text stream, raw file or None)."""
    if text is not None:
        return io.StringIO(text), None
    raw = open(path, "rb")
    stream = gzip.GzipFile(fileobj=raw) if path.name.lower().endswith(".gz") else raw
    enc = "utf-8-sig" if encoding == "utf-8" else encoding
    return io.TextIOWrapper(stream, encoding=enc, errors="strict", newline=""), raw


class _JsonReader:
    """Reads a JSON document in blocks so records can be decoded one by one."""

    def __init__(self, stream, tick):
        self.stream, self.tick = stream, tick
        self.buf, self.pos, self.eof = "", 0, False

    def more(self) -> bool:
        if self.eof:
            return False
        chunk = self.stream.read(1 << 22)
        self.tick()
        if not chunk:
            self.eof = True
            return False
        self.buf = self.buf[self.pos:] + chunk
        self.pos = 0
        return True

    def peek(self) -> str:
        """Next non-space character ('' at the end)."""
        while True:
            m = _NON_SPACE.search(self.buf, self.pos)
            if m:
                self.pos = m.start()
                return self.buf[self.pos]
            self.pos = len(self.buf)
            if not self.more():
                return ""

    def value(self):
        while True:
            try:
                v, end = _DECODER.raw_decode(self.buf, self.pos)
            except json.JSONDecodeError as e:
                # the value may continue in the next block: read more and retry
                if len(self.buf) - self.pos <= RECORD_LIMIT and self.more():
                    continue
                near = self.buf[e.pos:e.pos + 40].strip()
                if self.eof and (not near or e.pos >= len(self.buf) - 1 or e.msg.startswith("Unterminated")):
                    raise ConversionError("err_json_incomplete") from None
                raise ConversionError("err_json_malformed_near", near=near) from None
            if end >= len(self.buf) and self.more():
                continue          # the value reached the end of the block: it might have been cut
            self.pos = end
            return v

    def expect_colon(self, key: str):
        if self.peek() != ":":
            raise ConversionError("err_json_expected_colon", key=key)
        self.pos += 1


def _items(reader: _JsonReader, at_root: bool):
    """Items of a list whose «[» was already read: (object | None, number, text if not an object)."""
    number = 0
    if reader.peek() == "]":
        reader.pos += 1
        return
    while True:
        number += 1
        if reader.peek() == "":
            raise ConversionError("err_json_incomplete")
        v = reader.value()
        if isinstance(v, dict):
            yield v, number, None
        elif number == 1 and at_root:
            raise ConversionError("err_json_scalars")
        else:
            yield None, number, _json_text(v)[:80]
        c = reader.peek()
        if c == ",":
            reader.pos += 1
        elif c == "]":
            reader.pos += 1
            return
        else:
            raise ConversionError("err_json_after_item", n=number)


def _document_records(reader: _JsonReader, fmt: JsonFormat):
    c = reader.peek()
    if c == "[":
        reader.pos += 1
        yield from _items(reader, True)
    elif c == "{":
        reader.pos += 1
        flat, found, empty = {}, False, None
        if reader.peek() == "}":
            reader.pos += 1
        else:
            while True:
                if reader.peek() != '"':
                    raise ConversionError("err_json_expected_key")
                key = reader.value()
                reader.expect_colon(key)
                if reader.peek() == "[" and not found:
                    reader.pos += 1
                    c2 = reader.peek()
                    if c2 == "{":
                        found, fmt.key = True, key
                        yield from _items(reader, False)
                    elif c2 == "]":
                        reader.pos += 1
                        empty = key if empty is None else empty
                        flat[key] = []
                    else:
                        flat[key] = [v for v, *_ in _list_values(reader)]
                else:
                    v = reader.value()
                    if not found:
                        flat[key] = v
                c = reader.peek()
                if c == ",":
                    reader.pos += 1
                elif c == "}":
                    reader.pos += 1
                    break
                else:
                    raise ConversionError("err_json_incomplete")
        if not found:
            if empty is not None:
                fmt.key = empty
            else:
                fmt.single = True
                yield flat, 1, None
    elif c == "":
        raise ConversionError("err_empty_file")
    else:
        raise ConversionError("err_json_not_json")
    if reader.peek() != "":
        raise ConversionError("err_json_trailing")


def _list_values(reader: _JsonReader):
    """Values of an already opened list that is not the list of records."""
    while True:
        if reader.peek() == "":
            raise ConversionError("err_json_incomplete")
        yield (reader.value(),)
        c = reader.peek()
        if c == ",":
            reader.pos += 1
        elif c == "]":
            reader.pos += 1
            return
        else:
            raise ConversionError("err_json_in_list")


def _json_records(path: Path, fmt: JsonFormat, progress=None, cancel=None, text: str | None = None):
    """Yields (object | None, number, text) for every record."""
    stream, raw = _open_text(path, fmt.encoding, text)
    size = max(path.stat().st_size, 1) if raw else 1

    def tick():
        if cancel is not None and cancel.is_set():
            raise Cancelled()
        if progress and raw:
            progress(min(raw.tell() / size, 0.99))

    try:
        if fmt.mode == "lines":
            for n, line in enumerate(stream, 1):
                if n % 20000 == 0:
                    tick()
                line = line.strip()
                if not line:
                    continue
                try:
                    v = json.loads(line)
                except ValueError:
                    yield None, n, line[:80]
                    continue
                yield (v, n, None) if isinstance(v, dict) else (None, n, line[:80])
        else:
            yield from _document_records(_JsonReader(stream, tick), fmt)
    except UnicodeDecodeError as e:
        raise _NotUTF8("invalid UTF8") from e
    finally:
        stream.close()
        if raw:
            raw.close()


def _bad_record(fmt: JsonFormat, number: int, text: str | None) -> ConversionError:
    key = "err_json_bad_line" if fmt.mode == "lines" else "err_json_bad_item"
    return ConversionError(key, n=number, text=text or "…")


def _analyze_json(path: Path, fmt: JsonFormat, op: Options, progress=None, cancel=None, text=None,
                  maximum: int | None = None):
    """First pass: columns (in order of appearance), types and malformed records."""
    index: dict[str, int] = {}
    used: set[str] = set()
    guessers: list[_JsonTypeGuesser] = []
    rows = bad = 0
    records = [] if maximum else None
    flat: list = []
    try:
        for obj, number, txt in _json_records(path, fmt, progress, cancel, text):
            if obj is None:
                if op.skip_bad_rows:
                    bad += 1
                    continue
                raise _bad_record(fmt, number, txt)
            rows += 1
            flat.clear()
            _flatten(obj, "", flat)
            row = {} if records is not None else None
            for key, v, is_list in flat:
                k = index.get(key)
                if k is None:
                    k = len(fmt.keys)
                    name = key.replace("﻿", "").strip() or f"column_{k + 1}"
                    base, j = name, 2
                    while name.lower() in used:
                        name, j = f"{base}_{j}", j + 1
                    used.add(name.lower())
                    index[key] = k
                    fmt.keys.append(key)
                    fmt.names.append(name)
                    guessers.append(_JsonTypeGuesser(op.dmy_dates))
                if not op.all_text:
                    guessers[k].probe(v, is_list, rows)
                if row is not None:
                    row[k] = v
            if records is not None:
                records.append(row)
                if len(records) >= maximum:
                    break
    except ConversionError:
        if text is None or not records:      # in the preview, the end of the sample may be cut
            raise
    if not fmt.names:
        raise ConversionError("err_json_empty_objects" if rows else "err_json_no_records")
    kinds = ["string"] * len(fmt.names) if op.all_text else [g.kind() for g in guessers]
    mixed = [(fmt.names[j], g.mixed) for j, g in enumerate(guessers) if g.mixed and kinds[j] == "string"]
    return kinds, [g.zone for g in guessers], rows, bad, mixed, records


def _json_table(columns: list[list], kinds: list[str], zones: list[bool], names: list[str]) -> pa.Table:
    arrays = [pa.array(col, type=_arrow_type(k, z)) for col, k, z in zip(columns, kinds, zones)]
    return pa.Table.from_arrays(arrays, names=names)


def _mixed_note(mixed) -> Msg:
    items = MsgList((Msg("note_mixed_item", column=c, value=v[:30], row=n) for c, (v, n) in mixed), "; ")
    return Msg("note_mixed_columns", items=items)


def _convert_json(source, target, op, r, progress, cancel, source_label, division):
    fmt = detect_json_format(source, op)
    for attempt in range(2):
        try:
            fmt.names, fmt.keys, fmt.key, fmt.single = [], [], None, False
            kinds, zones, rows, bad, mixed, _ = _analyze_json(
                source, fmt, op, (lambda f: progress(f * 0.35)) if progress else None, cancel)
            break
        except _NotUTF8:
            if op.encoding == "auto" and fmt.encoding == "utf-8" and attempt == 0:
                fmt.encoding = "cp1252"
                continue
            raise ConversionError("err_not_utf8") from None
    r.format, r.encoding = fmt.describe(), fmt.encoding
    kinds = ["string" if k == "null" else k for k in kinds]
    names = list(fmt.names)
    schema = pa.schema([pa.field(n, _arrow_type(k, z)) for n, k, z in zip(names, kinds, zones)])
    if source_label is not None:
        schema = schema.append(pa.field(SOURCE_COLUMN, pa.string()))
    convert = [_json_converter(k, op.dmy_dates) for k in kinds]
    ncols = len(names)
    batch = max(5_000, min(100_000, 1_000_000 // max(ncols, 1)))
    output = ParquetOutput(target, schema, op.compression, division)
    columns = [[] for _ in range(ncols)]
    index = {c: k for k, c in enumerate(fmt.keys)}
    flat: list = []
    written = 0

    def flush():
        nonlocal columns
        if not columns[0] and ncols:
            return
        n = len(columns[0])
        table = _json_table(columns, kinds, zones, names)
        if source_label is not None:
            table = table.append_column(SOURCE_COLUMN, pa.repeat(source_label, n))
        output.write(table)
        columns = [[] for _ in range(ncols)]

    try:
        tick = (lambda f: progress(0.35 + 0.65 * f)) if progress else None
        for obj, number, txt in _json_records(source, fmt, tick, cancel):
            if obj is None:
                if op.skip_bad_rows:
                    continue
                raise _bad_record(fmt, number, txt)
            row = [None] * ncols
            flat.clear()
            _flatten(obj, "", flat)
            for key, v, _ in flat:
                k = index.get(key)
                if k is not None:
                    row[k] = v
            for k in range(ncols):
                v = row[k]
                if v is not None:
                    try:
                        v = convert[k](v)
                    except (ValueError, TypeError):
                        raise ConversionError("err_value_type", value=str(v)[:60], column=names[k],
                                              row=written + 1) from None
                columns[k].append(v)
            written += 1
            if len(columns[0]) >= batch:
                flush()
        flush()
        r.parquet_bytes, parts = output.close()
        output = None
    finally:
        if output is not None:
            output.abort()
    if progress:
        progress(1.0)
    r.parts = parts if division else 0
    r.rows, r.skipped_rows = written, bad
    r.columns = ncols + (1 if source_label is not None else 0)
    r.status = "ok"
    notes = []
    if mixed:
        notes.append(_mixed_note(mixed))
    if bad:
        notes.append(Msg("note_skipped_records", n=bad))
    r.text_columns = [c for c, _ in mixed]
    r.messages = notes


def preview_json(path: Path, op: Options, rows: int = 200) -> tuple[pa.Table, JsonFormat]:
    fmt = detect_json_format(path, op)
    sample = read_sample(path)
    text = sample.decode(fmt.encoding, errors="replace").lstrip("﻿")
    kinds, zones, _, _, _, records = _analyze_json(path, fmt, op, text=text, maximum=1000)
    if not records:
        raise ConversionError("err_json_preview_no_record")
    kinds = ["string" if k == "null" else k for k in kinds]
    convert = [_json_converter(k, op.dmy_dates) for k in kinds]
    columns = [[] for _ in fmt.names]
    for row in records[:rows]:
        for k in range(len(fmt.names)):
            v = row.get(k)
            if v is not None:
                try:
                    v = convert[k](v)
                except (ValueError, TypeError):
                    v = None
            columns[k].append(v)
    return _json_table(columns, kinds, zones, fmt.names), fmt


# --------------------------------------------------------------------------- #
#  Combining several files into one Parquet
# --------------------------------------------------------------------------- #
def _common_type(types: list[pa.DataType]) -> pa.DataType:
    types = [t for t in types if not pa.types.is_null(t)]
    if not types:
        return pa.string()
    if all(t == types[0] for t in types):
        return types[0]
    if all(pa.types.is_date(t) or pa.types.is_timestamp(t) for t in types):
        zones = {t.tz for t in types if pa.types.is_timestamp(t) and t.tz}
        return pa.timestamp("ms", tz="UTC" if zones else None)
    try:
        schemas = [pa.schema([("x", t)]) for t in types]
        return pa.unify_schemas(schemas, promote_options="permissive").field("x").type
    except (pa.ArrowTypeError, pa.ArrowInvalid, pa.ArrowNotImplementedError):
        return pa.string()


def combine(parts: list[Path], target: Path, op: Options,
            cancel: threading.Event | None = None,
            progress: Callable[[float], None] | None = None) -> tuple[int, list[str], int, int]:
    """Merges several temporary Parquet files into one (or its parts) with a common schema."""
    schemas = [pq.read_schema(str(p)) for p in parts]
    order: list[str] = []
    types: dict[str, list[pa.DataType]] = {}
    for schema in schemas:
        for f in schema:
            if f.name not in types:
                order.append(f.name)
                types[f.name] = []
            types[f.name].append(f.type)
    if SOURCE_COLUMN in order:                 # the source column always goes last
        order.remove(SOURCE_COLUMN)
        order.append(SOURCE_COLUMN)
    final = pa.schema([(n, _common_type(types[n])) for n in order])
    as_text = [n for n in order if pa.types.is_string(final.field(n).type)
               and any(not pa.types.is_string(t) and not pa.types.is_null(t) for t in types[n])]

    rows = 0
    writer = ParquetOutput(target, final, op.compression, split_of(op))
    try:
        for i, part in enumerate(parts):
            file = pq.ParquetFile(str(part))
            for g in range(file.num_row_groups):
                if cancel is not None and cancel.is_set():
                    raise Cancelled()
                table = file.read_row_group(g)
                missing = [n for n in order if n not in table.column_names]
                for n in missing:
                    table = table.append_column(n, pa.nulls(table.num_rows, final.field(n).type))
                table = table.select(order).cast(final)
                writer.write(table)
                rows += table.num_rows
            if progress:
                progress((i + 1) / len(parts))
        total, nparts = writer.close()
        writer = None
    finally:
        if writer is not None:
            writer.abort()
    return rows, as_text, total, nparts


def _temp_folder(target: Path) -> Path:
    tmp = target.parent / f".{target.stem}_parts_tmp"
    if tmp.exists():
        shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    return tmp


# --------------------------------------------------------------------------- #
#  Whole batch (used by the interface and the command line)
# --------------------------------------------------------------------------- #
Notify = Callable[..., None]


def combined_target(jobs: list[tuple[Path, Path]], out_dir: Path | None, op: Options) -> Path:
    name = op.combined_name.strip() or "combined.parquet"
    if not name.lower().endswith(".parquet"):
        name += ".parquet"
    return (out_dir or jobs[0][1]) / name


def convert_batch(jobs: list[tuple[Path, Path]], out_dir: Path | None, op: Options,
                  notify: Notify = lambda *a: None,
                  cancel: threading.Event | None = None) -> list[Result]:
    """
    Converts every job [(file, root)]. Reports progress with notify(event, *data):
    ("start", i) ("progress", i, fraction) ("done", i, Result) ("log", Msg, level)
    ("combining",) ("combined", bytes, parts).
    """
    results: list[Result] = []
    final = combined_target(jobs, out_dir, op) if op.combine else None
    if final is not None and output_exists(final, op) and not op.overwrite:
        notify("log", Msg("log_combined_exists", path=str(final)), "error")
        return results
    tmp = _temp_folder(final) if final is not None else None
    parts: list[Path] = []
    try:
        for i, (file, root) in enumerate(jobs):
            if cancel is not None and cancel.is_set():
                break
            notify("start", i)
            if final is not None:
                target = tmp / f"{i:05d}.parquet"
                label = file.relative_to(root).as_posix() if op.source_column else None
            else:
                target = target_path(file, root, out_dir, op.keep_structure)
                label = None
            r = convert_file(file, target, op,
                             progress=lambda f, i=i: notify("progress", i, f),
                             cancel=cancel, source_label=label, split=final is None)
            if final is not None and r.status == "ok":
                parts.append(target)
                r.target, r.parquet_bytes = final, 0
            results.append(r)
            notify("done", i, r)
            if r.status == "cancelled":
                break

        if final is not None and parts and not (cancel is not None and cancel.is_set()):
            notify("combining")
            notify("log", Msg("log_combining", n=len(parts), name=final.name), "info")
            try:
                rows, as_text, size, nparts = combine(parts, final, op, cancel)
                if split_of(op):
                    notify("log", Msg("log_combined_parts", path=str(parts_folder(final)),
                                      parts=Msg("parts_count", n=nparts), rows=Msg("rows_count", n=rows),
                                      size=size), "ok")
                else:
                    notify("log", Msg("log_combined", path=str(final), rows=Msg("rows_count", n=rows), size=size), "ok")
                notify("combined", size, nparts if split_of(op) else 0)
                if as_text:
                    notify("log", Msg("log_combined_text_columns", columns=", ".join(as_text)), "warning")
            except Cancelled:
                notify("log", Msg("log_combine_cancelled"), "error")
            except Exception as e:  # noqa: BLE001
                notify("log", Msg("log_combine_failed", error=error_message(e)), "error")
    finally:
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)
    return results


# --------------------------------------------------------------------------- #
#  Command line
# --------------------------------------------------------------------------- #
def _glob_base(pattern: str) -> Path:
    """The fixed part of a glob pattern: "data/**/*.csv" -> "data"."""
    fixed = []
    for part in re.split(r"[\\/]", pattern):
        if any(c in part for c in "*?["):
            break
        fixed.append(part)
    base = "/".join(fixed)
    return Path(base) if base else Path(".")


def collect_jobs(items: Iterable[str | Path], recursive: bool = True) -> tuple[list[tuple[Path, Path]], list[str]]:
    """
    [(file, root)] for files, folders and glob patterns ("**" matches subfolders),
    plus the items that matched nothing. For a pattern the root is its fixed part,
    so the output can keep the folder structure below it.
    """
    import glob

    jobs: list[tuple[Path, Path]] = []
    seen: set[Path] = set()
    unmatched: list[str] = []
    for item in items:
        item = str(item)
        if any(c in item for c in "*?["):
            base = _glob_base(item)
            found: list[tuple[Path, Path]] = []
            for match in sorted(glob.glob(item, recursive=True)):
                m = Path(match)
                if m.is_dir():
                    found += [(f, base) for f, _ in list_files([m], recursive)]
                elif is_supported(m):
                    found.append((m, base))
        else:
            found = list_files([Path(item)], recursive)
        if not found:
            unmatched.append(item)
        for file, root in found:
            key = file.resolve()
            if key not in seen:
                seen.add(key)
                jobs.append((file, root))
    return jobs, unmatched


def build_parser():
    import argparse

    ap = argparse.ArgumentParser(prog="engine.py", description="Convert CSV and JSON files to Apache Parquet.")
    ap.add_argument("inputs", nargs="+", help="CSV / JSON files, folders or glob patterns (** for subfolders)")
    ap.add_argument("-o", "--output", type=Path, help="output folder (default: next to each file)")
    ap.add_argument("--no-recursive", action="store_true", help="do not look inside subfolders")
    ap.add_argument("--flat", action="store_true", help="with --output, do not recreate the source subfolders")
    ap.add_argument("--overwrite", action="store_true", help="replace Parquet files that already exist")
    ap.add_argument("--compression", default="snappy", choices=list(COMPRESSIONS))
    reading = ap.add_argument_group("reading (detected automatically by default)")
    reading.add_argument("--sep", help="CSV separator: , ; | or tab")
    reading.add_argument("--encoding", default="auto", choices=list(ENCODINGS))
    reading.add_argument("--decimal", default="auto", choices=list(DECIMALS))
    reading.add_argument("--no-header", action="store_true", help="the first CSV row is data, not column names")
    reading.add_argument("--no-dmy-dates", action="store_true", help="do not read dd/mm/yyyy as dates")
    reading.add_argument("--all-text", action="store_true", help="store every column as text")
    reading.add_argument("--skip-bad-rows", action="store_true", help="skip malformed CSV rows / JSON Lines")
    output = ap.add_argument_group("output")
    output.add_argument("--combine", metavar="NAME.parquet", help="combine all files into one Parquet")
    output.add_argument("--no-source-column", action="store_true",
                        help=f"with --combine, do not add the \"{SOURCE_COLUMN}\" column")
    group = output.add_mutually_exclusive_group()
    group.add_argument("--split-rows", type=int, metavar="N", help="split into parts of at most N rows")
    group.add_argument("--split-mb", type=float, metavar="N", help="split into parts of about N MB")
    ap.add_argument("--lang", choices=list(i18n.LANGUAGES), help="language of the messages (default: system)")
    return ap


def options_from_args(a) -> Options:
    sep = {"tab": "\t", "\\t": "\t"}.get(a.sep, a.sep) if a.sep else None
    return Options(separator=sep, encoding=a.encoding, decimal=a.decimal, header=not a.no_header,
                   dmy_dates=not a.no_dmy_dates, all_text=a.all_text, skip_bad_rows=a.skip_bad_rows,
                   compression=a.compression, overwrite=a.overwrite, recursive=not a.no_recursive,
                   keep_structure=not a.flat, combine=bool(a.combine), combined_name=a.combine or "",
                   source_column=bool(a.combine) and not a.no_source_column,
                   split_mode="rows" if a.split_rows else "mb" if a.split_mb else None,
                   split_value=a.split_rows or a.split_mb or 0)


def main(argv: list[str] | None = None) -> int:
    a = build_parser().parse_args(argv)
    i18n.set_language(a.lang or i18n.detect_language())
    op = options_from_args(a)
    jobs, unmatched = collect_jobs(a.inputs, op.recursive)
    for item in unmatched:
        print(i18n.t("cli_no_match", pattern=item), file=sys.stderr)
    if not jobs:
        print(i18n.t("cli_no_files"), file=sys.stderr)
        return 1

    def notify(event, *data):
        if event == "done":
            r = data[1]
            parts = " " + i18n.t("cli_in_parts", n=r.parts) if r.parts else ""
            status = i18n.t(f"status_{r.status}").upper()
            print(f"[{status:9}] {r.source}  ->  {i18n.t('rows_count', n=r.rows)}{parts}  {r.message}")
        elif event == "log":
            print(str(data[0]))

    results = convert_batch(jobs, a.output, op, notify)
    return 1 if any(r.status == "error" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
