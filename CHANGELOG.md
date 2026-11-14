# Changelog

## [0.1.0-RC1]
First release candidate.
### Added
- `Reader`: flat and nested columns (structs, lists, maps, legacy forms), logical types converted to exact PHP values, every RFC encoding
  except ALP, the codecs UNCOMPRESSED, SNAPPY, GZIP, LZ4, LZ4_RAW, ZSTD and BROTLI in pure PHP, statistics, page indexes, row ranges and
  `candidateRanges()`, all through a range function (files, strings, HTTP range requests).
- `Writer`: rows to Parquet bytes with PLAIN and RLE_DICTIONARY encodings, SNAPPY/GZIP/ZSTD, statistics and page indexes; `Type::infer()`.
