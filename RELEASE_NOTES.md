First release of **CSV and JSON to Parquet**: convert CSV and JSON files to Apache Parquet inside your workflows.

- Reads CSV, TSV, JSON, JSON Lines and NDJSON, also `.gz` compressed.
- Detects the separator, encoding (UTF-8 / Windows-1252), decimal comma and column types; nested JSON is flattened into `parent.child` columns.
- Splits the output into parts by rows or size, or combines many files into one Parquet with a `source_file` column.
- Snappy, Zstandard or Gzip compression.
- Job summary, error and warning annotations, and outputs with counts and the list of files written.
- Runs on Linux, Windows and macOS runners.

```yaml
- uses: alcazarfran77-dev/csv-to-parquet-action@v1
  with:
    path: data/**/*.csv
    output: parquet
```
