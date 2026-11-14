# Changelog

## [0.1.0-RC2]
### Changed
- Exceptions follow the charm exception policy (modelled on ext/json). `ParquetException` now extends `\UnexpectedValueException`
  (it was `\RuntimeException`) and is the exception for a corrupt or truncated file; `UnsupportedException` still extends it.
  `catch (\UnexpectedValueException)` catches a data error from any charm package.
- Misuse is a built-in error: an unknown column, a row group out of range, a negative offset or limit, a malformed row range
  (`\InvalidArgumentException` / `\OutOfRangeException` before) and a codec the Writer cannot write (`UnsupportedException` before)
  are `\ValueError`; any call to a finished `Writer` is `\Error` (`\LogicException` before).
- `Reader::fromFile()` on a file that is missing or cannot be read throws `\RuntimeException` (`ParquetException` before).
- A range function that returns the wrong number of bytes (or a non-string) is an `\UnexpectedValueException`
  (`ParquetException` before). Exceptions the range function throws pass through unchanged, as before.
- A codec failure from charm/zstd or charm/brotli becomes a `ParquetException` with the original as its previous exception.
- Requires charm/zstd and charm/brotli 0.1.0-RC2 (which also brings their streaming APIs and charm/zstd's PHP 8.4+ JIT fixes).
### Removed
- `FormatException`: it is merged into `ParquetException`. Replace `catch (FormatException)` with `catch (ParquetException)`.
  (Catching `ParquetException` now also catches `UnsupportedException`, as it always did; catch `UnsupportedException` first to tell them apart.)

## [0.1.0-RC1]
First release candidate.
### Added
- `Reader`: flat and nested columns (structs, lists, maps, legacy forms), logical types converted to exact PHP values, every RFC encoding
  except ALP, the codecs UNCOMPRESSED, SNAPPY, GZIP, LZ4, LZ4_RAW, ZSTD and BROTLI in pure PHP, statistics, page indexes, row ranges and
  `candidateRanges()`, all through a range function (files, strings, HTTP range requests).
- `Writer`: rows to Parquet bytes with PLAIN and RLE_DICTIONARY encodings, SNAPPY/GZIP/ZSTD, statistics and page indexes; `Type::infer()`.
