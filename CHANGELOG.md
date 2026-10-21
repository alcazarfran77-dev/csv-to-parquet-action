# Changelog

## v1.0.0

- First release: converts CSV, TSV, JSON and JSON Lines (also `.gz`) to Apache Parquet.
- Automatic detection of separator, encoding, decimal comma and column types; nested JSON is flattened.
- Split into parts by rows or size, combine many files into one, Snappy / Zstandard / Gzip compression.
- Job summary, error and warning annotations, outputs with counts and the list of files written.
- Log and summary in English or Spanish.
