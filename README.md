# CSV and JSON to Parquet

A GitHub Action that converts **CSV and JSON** files to **Apache Parquet** inside your workflow. It detects separators, encodings, decimal commas and column types on its own, reads JSON lists, JSON Lines and wrapped records, and can split the output into parts or combine many files into one.

It runs the same engine as the [CSV to Parquet](https://github.com/alcazarfran77-dev/csv-to-parquet) desktop and browser app, on Linux, Windows and macOS runners.

## Usage

```yaml
- uses: alcazarfran77-dev/csv-to-parquet-action@v1
  with:
    path: data/**/*.csv
    output: parquet
```

### Convert and keep the result as a workflow artifact

```yaml
name: Export to Parquet
on: [workflow_dispatch]

jobs:
  convert:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: alcazarfran77-dev/csv-to-parquet-action@v1
        id: parquet
        with:
          path: |
            exports/*.csv
            events/**/*.jsonl
          output: parquet
          compression: zstd
      - run: echo "Converted ${{ steps.parquet.outputs.converted }} files, ${{ steps.parquet.outputs.rows }} rows"
      - uses: actions/upload-artifact@v7
        with:
          name: parquet
          path: parquet/
```

### Combine every file into one dataset, split into parts

```yaml
- uses: alcazarfran77-dev/csv-to-parquet-action@v1
  with:
    path: data/daily
    output: warehouse
    combine: sales.parquet       # adds a "source_file" column
    split-rows: 1000000          # warehouse/sales/sales_0001.parquet, …
```

### Spanish Excel exports (semicolon, decimal comma, dd/mm/yyyy)

Nothing to configure: they are detected. To force the reading options:

```yaml
- uses: alcazarfran77-dev/csv-to-parquet-action@v1
  with:
    path: exports/*.csv
    separator: ";"
    decimal: ","
    encoding: cp1252
    language: es                 # log and job summary in Spanish
```

Each run writes a **job summary** with one row per file (status, rows, columns, size and notes), adds an **error annotation** to every file that could not be converted and a **warning** when, for example, a column was stored as text because it mixed numbers and text.

## Inputs

| Input | Default | Description |
|---|---|---|
| `path` | *(required)* | Files, folders or glob patterns, one per line. `**` matches subfolders. Reads `.csv`, `.tsv`, `.json`, `.jsonl`, `.ndjson` and their `.gz` versions. |
| `output` | *(next to each file)* | Folder for the Parquet files. |
| `recursive` | `true` | Look inside the subfolders of the folders given in `path`. |
| `keep-structure` | `true` | With `output`, recreate the source subfolders below it (relative to the folder or to the fixed part of the pattern). |
| `overwrite` | `false` | Replace Parquet files that already exist. Otherwise they are skipped. |
| `compression` | `snappy` | `snappy`, `zstd`, `gzip` or `none`. |
| `separator` | `auto` | CSV separator: `auto`, `,`, `;`, `\|` or `tab`. |
| `encoding` | `auto` | `auto`, `utf-8`, `cp1252`, `latin-1` or `utf-16`. |
| `decimal` | `auto` | CSV decimal mark: `auto`, `.` or `,`. |
| `header` | `true` | The first CSV row has the column names. With `false` they are called `column_1`, `column_2`… |
| `dmy-dates` | `true` | Read `31/12/2025` as a date. Set `false` for mm/dd/yyyy data. |
| `all-text` | `false` | Store every column as text (no type detection). |
| `skip-bad-rows` | `false` | Skip malformed CSV rows and JSON Lines instead of failing the file. |
| `combine` | | Combine all the files into one Parquet with this name, e.g. `all.parquet`. |
| `source-column` | `true` | With `combine`, add a `source_file` column. |
| `split-rows` | | Split each Parquet into parts of at most this many rows. |
| `split-mb` | | Split each Parquet into parts of about this many MB. |
| `fail-on-error` | `true` | Fail the step if any file could not be converted. |
| `language` | `en` | Language of the log and the job summary: `en` or `es`. |
| `python-version` | `3.12` | Python used to run the converter. Empty = use the Python already on the runner. |

## Outputs

| Output | Description |
|---|---|
| `converted` | Number of files converted. |
| `failed` | Number of files that could not be converted. |
| `skipped` | Number of files skipped because their Parquet already existed. |
| `rows` | Total number of rows written. |
| `files` | The Parquet files written, one per line (relative to the workspace). |

## What it detects

**CSV** — separator (`,` `;` tab `|`), encoding (UTF-8 or Windows-1252, with automatic retry), decimal comma, integers, decimals, dates (ISO and `dd/mm/yyyy`), timestamps and booleans. Messy headers are cleaned (`column_3` for empty names, `name_2` for duplicates). A column that changes type halfway through the file is stored as text and reported.

**JSON** — a list of objects, JSON Lines, or an object that wraps the list (`{"data": [ … ]}`). Nested objects become dotted columns (`customer.name`), inner lists are stored as JSON text, missing keys become empty values, dates in strings become dates, and integers that don't fit in 64 bits are stored as text.

## Splitting

With `split-rows` or `split-mb`, each result is written as parts inside a folder named after it:

```
parquet/sales/sales_0001.parquet
parquet/sales/sales_0002.parquet
```

DuckDB (`SELECT * FROM 'parquet/sales/*.parquet'`), pandas (`pd.read_parquet('parquet/sales')`), Polars and Spark read the folder as one table. When overwriting, parts left over from a previous, larger run are removed.

## Notes

- `pyarrow` is installed in a private folder under the runner's temporary directory, so your job's Python environment is not modified.
- Files are read and written in blocks: files larger than the runner's memory work.
- A half-written Parquet is never left behind when a file fails.

## License

[MIT](LICENSE). Uses [Apache Arrow](https://arrow.apache.org/) (`pyarrow`, Apache-2.0).
