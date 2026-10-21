"""Tests of run_action.py: inputs, outputs, summary and exit code."""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import run_action  # noqa: E402

INPUTS = ["PATH", "OUTPUT", "RECURSIVE", "KEEP_STRUCTURE", "OVERWRITE", "COMPRESSION", "SEPARATOR", "ENCODING",
          "DECIMAL", "HEADER", "DMY_DATES", "ALL_TEXT", "SKIP_BAD_ROWS", "COMBINE", "SOURCE_COLUMN", "SPLIT_ROWS",
          "SPLIT_MB", "FAIL_ON_ERROR", "LANGUAGE"]


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    shutil.copytree(HERE / "data", tmp_path / "data")
    monkeypatch.chdir(tmp_path)
    for name in INPUTS:
        monkeypatch.delenv("CTP_" + name, raising=False)
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "output.txt"))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary.md"))
    return tmp_path


def outputs(ws: Path) -> dict[str, str]:
    values, lines = {}, iter((ws / "output.txt").read_text(encoding="utf-8").splitlines())
    for line in lines:
        if "<<" in line:
            key, delimiter = line.split("<<", 1)
            block = []
            for item in lines:
                if item == delimiter:
                    break
                block.append(item)
            values[key] = "\n".join(block)
        else:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def test_convert_and_split(workspace, monkeypatch, capsys):
    monkeypatch.setenv("CTP_PATH", "data/*.csv\ndata/*.json\ndata/sub\nmissing/*.csv")
    monkeypatch.setenv("CTP_OUTPUT", "out")
    monkeypatch.setenv("CTP_SPLIT_ROWS", "150")
    assert run_action.main() == 1                      # malformed.csv fails
    out = outputs(workspace)
    assert (out["converted"], out["failed"], out["rows"]) == ("6", "1", "1500")
    assert "out/list/list_0003.parquet" in out["files"].splitlines()
    log = capsys.readouterr().out
    assert "::warning::Nothing matches “missing/*.csv”." in log
    assert "::error file=data/malformed.csv::A row has 4 columns" in log
    summary = (workspace / "summary.md").read_text(encoding="utf-8")
    assert "| data/sales.csv | OK | 300 | 5 |" in summary


def test_fail_on_error_false(workspace, monkeypatch):
    monkeypatch.setenv("CTP_PATH", "data/malformed.csv")
    monkeypatch.setenv("CTP_FAIL_ON_ERROR", "false")
    assert run_action.main() == 0


def test_combine_in_spanish(workspace, monkeypatch, capsys):
    monkeypatch.setenv("CTP_PATH", "data")
    monkeypatch.setenv("CTP_OUTPUT", "out")
    monkeypatch.setenv("CTP_COMBINE", "all.parquet")
    monkeypatch.setenv("CTP_SKIP_BAD_ROWS", "true")
    monkeypatch.setenv("CTP_LANGUAGE", "es")
    assert run_action.main() == 0
    t = pq.read_table(workspace / "out/all.parquet")
    assert t.num_rows == 1502 and t.column_names[-1] == "source_file"
    assert outputs(workspace)["files"] == "out/all.parquet"
    assert "Se omitió 1 fila mal formada." in capsys.readouterr().out
    assert "1.502 filas" in (workspace / "summary.md").read_text(encoding="utf-8")
    assert run_action.main() == 1                      # exists now, and overwrite is false


def test_tab_separator_and_options(workspace, monkeypatch):
    monkeypatch.setenv("CTP_PATH", "data/sub/defects.tsv")
    monkeypatch.setenv("CTP_SEPARATOR", "tab")
    monkeypatch.setenv("CTP_ALL_TEXT", "true")
    monkeypatch.setenv("CTP_COMPRESSION", "GZIP")
    assert run_action.main() == 0
    f = pq.ParquetFile(workspace / "data/sub/defects.parquet")
    assert f.metadata.row_group(0).column(0).compression == "GZIP"
    assert all(str(field.physical_type) == "BYTE_ARRAY" for field in f.schema)


@pytest.mark.parametrize("name,value", [("COMPRESSION", "lz4"), ("OVERWRITE", "maybe"), ("SPLIT_ROWS", "-5"),
                                        ("SEPARATOR", "::")])
def test_invalid_inputs(workspace, monkeypatch, capsys, name, value):
    monkeypatch.setenv("CTP_PATH", "data")
    monkeypatch.setenv("CTP_" + name, value)
    assert run_action.main() == 1
    assert "::error::Invalid value" in capsys.readouterr().out


def test_nothing_to_convert(workspace, monkeypatch, capsys):
    monkeypatch.setenv("CTP_PATH", "nowhere/*.csv")
    assert run_action.main() == 1
    assert "::error::No CSV or JSON files found." in capsys.readouterr().out
