<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\{Codec, ColumnChunk, Encoding, ParquetException, PageType, PhysicalType, UnsupportedException};
use Charm\Parquet\Thrift\CompactReader;

/**
 * Decodes one flat column chunk (maximum repetition level 0, maximum definition level 0 or 1) into a list of values, null where missing.
 *
 * Every size that comes from the file is checked before it is used for an allocation or a read: the chunk against the file (by the
 * caller), each page against the chunk, page sizes against $maxPageSize, value counts against the row group.
 */
final class ColumnReader
{
    /**
     * A flat column (maximum repetition level 0, maximum definition level 0 or 1): one entry per row, null where missing.
     *
     * @param string $chunk the bytes of the chunk: its pages, back to back
     * @return list<bool|int|float|string|null>
     */
    public static function read(string $chunk, ColumnChunk $meta, int $rowGroupRows, int $maxPageSize): array
    {
        if ($meta->numValues !== $rowGroupRows) {
            throw new ParquetException("Column '{$meta->leaf->dottedPath}' has {$meta->numValues} values in a row group of $rowGroupRows rows");
        }
        return self::run($chunk, $meta, $maxPageSize, true)[0];
    }

    /**
     * Any column, in its shredded form: the non-null values densely, and the definition and repetition levels of every entry
     * (null for a level that the schema does not have). The number of entries is the chunk's num_values. Levels are checked against
     * the maxima, the chunk must start with repetition level 0, and the number of records (entries with repetition level 0) must equal
     * $rowGroupRows.
     *
     * @return array{list<bool|int|float|string>, list<int>|null, list<int>|null}
     */
    public static function shred(string $chunk, ColumnChunk $meta, int $rowGroupRows, int $maxPageSize): array
    {
        [$vals, $defs, $reps] = self::run($chunk, $meta, $maxPageSize, false);
        $leaf = $meta->leaf;
        if ($reps !== null) {
            if ($reps !== [] && $reps[0] !== 0) {
                throw new ParquetException("Column '{$leaf->dottedPath}' starts with repetition level {$reps[0]}: a chunk must begin a record");
            }
            $records = \array_count_values($reps)[0] ?? 0;
        } else {
            $records = $meta->numValues;
        }
        if ($records !== $rowGroupRows) {
            throw new ParquetException("Column '{$leaf->dottedPath}' holds $records records in a row group of $rowGroupRows rows");
        }
        return [$vals, $defs, $reps];
    }

    /**
     * Some pages of a column chunk (the dictionary page, if any, then consecutive data pages). The first data page starts a row (the
     * offset index promises it: verified here for repeated columns). Like shred(), for $rows rows.
     *
     * @return array{list<bool|int|float|string>, list<int>|null, list<int>|null}
     */
    public static function shredPages(string $pages, ColumnChunk $meta, int $rows, int $maxPageSize): array
    {
        [$vals, $defs, $reps] = self::run($pages, $meta, $maxPageSize, false, true);
        $leaf = $meta->leaf;
        $records = $reps !== null ? (\array_count_values($reps)[0] ?? 0) : \count($defs ?? $vals);
        // (only the first page is one that the reader started at: later ones may continue its last row)
        if ($reps !== null && ($reps === [] || $reps[0] !== 0)) {
            throw new ParquetException("Column '{$leaf->dottedPath}': a page that the offset index says starts a row starts with repetition level " . ($reps[0] ?? 'none'));
        }
        if ($records !== $rows) {
            throw new ParquetException("Column '{$leaf->dottedPath}': the selected pages hold $records rows, the offset index says $rows");
        }
        return [$vals, $defs, $reps];
    }

    /**
     * @param bool $partial the bytes are some pages of the chunk: read them all, whatever num_values says
     * @return array{list<mixed>, list<int>|null, list<int>|null} values (merged with nulls when $merge), definition levels, repetition levels
     */
    private static function run(string $chunk, ColumnChunk $meta, int $maxPageSize, bool $merge, bool $partial = false): array
    {
        $leaf = $meta->leaf;
        $name = $leaf->dottedPath;
        $maxDef = $leaf->maxDefinitionLevel;
        $maxRep = $leaf->maxRepetitionLevel;
        $type = $meta->physicalType;
        $total = $meta->numValues;
        if ($partial) {
            $total = \PHP_INT_MAX;
        } elseif ($total === 0) {
            return [[], $maxDef > 0 ? [] : null, $maxRep > 0 ? [] : null];
        }
        $codec = $meta->codec ?? throw new UnsupportedException('codec ' . $meta->codecId);
        Compression::assertSupported($codec); // (before any page: an unreadable codec is the reason, not the size of its pages)
        $chunkLen = \strlen($chunk);
        $pos = 0;
        $dict = null;
        $dictCount = 0;
        $seen = 0;
        $parts = [];
        $defParts = [];
        $repParts = [];

        while ($partial ? $pos < $chunkLen : $seen < $total) {
            if ($pos >= $chunkLen) {
                throw new ParquetException("Column '$name' ends after $seen of $total values");
            }
            $r = new CompactReader($chunk, $pos);
            $h = $r->readStruct();
            $pos = $r->position();
            $pageType = PageType::tryFrom(Fields::int($h, 1, 'PageHeader.type')) ?? throw new UnsupportedException('page type ' . Fields::int($h, 1, 'PageHeader.type'));
            $uncompressed = Fields::int($h, 2, 'PageHeader.uncompressed_page_size');
            $compressed = Fields::int($h, 3, 'PageHeader.compressed_page_size');
            if ($uncompressed < 0 || $compressed < 0 || $compressed > $chunkLen - $pos) {
                throw new ParquetException("Column '$name' has a page that is larger than its column chunk");
            }
            if ($uncompressed > $maxPageSize) {
                throw new UnsupportedException("page of $uncompressed bytes above the page size limit of $maxPageSize");
            }
            $body = \substr($chunk, $pos, $compressed);
            $pos += $compressed;

            if ($pageType === PageType::IndexPage) {
                continue;
            }

            if ($pageType === PageType::DictionaryPage) {
                if ($dict !== null || $seen > 0) {
                    throw new ParquetException("Column '$name' has a dictionary page after another dictionary or after data");
                }
                $dh = Fields::struct($h, 7, 'PageHeader.dictionary_page_header');
                $n = Fields::int($dh, 1, 'DictionaryPageHeader.num_values');
                $enc = Fields::int($dh, 2, 'DictionaryPageHeader.encoding');
                if ($enc !== Encoding::Plain->value && $enc !== Encoding::PlainDictionary->value) {
                    throw new UnsupportedException('dictionary page encoding ' . self::encodingName($enc));
                }
                if ($n < 0 || $n > $uncompressed * 8) {
                    throw new ParquetException("Column '$name' has a dictionary of $n values");
                }
                $raw = Compression::decompress($codec, $body, $uncompressed);
                $dict = PlainDecoder::decode($raw, 0, $type, $n, $leaf->typeLength);
                $dictCount = $n;
                continue;
            }

            // a data page: v1 keeps the levels inside the compressed body, v2 keeps them in front of it, uncompressed
            if ($pageType === PageType::DataPage) {
                $dh = Fields::struct($h, 5, 'PageHeader.data_page_header');
                $nv = Fields::int($dh, 1, 'DataPageHeader.num_values');
                $enc = Fields::int($dh, 2, 'DataPageHeader.encoding');
                $defEnc = Fields::int($dh, 3, 'DataPageHeader.definition_level_encoding');
            } else {
                $dh = Fields::struct($h, 8, 'PageHeader.data_page_header_v2');
                $nv = Fields::int($dh, 1, 'DataPageHeaderV2.num_values');
                $enc = Fields::int($dh, 4, 'DataPageHeaderV2.encoding');
            }
            if ($nv < 0 || $nv > $total - $seen) {
                throw new ParquetException("Column '$name' has a page of $nv values, but only " . ($total - $seen) . ' are left in the chunk');
            }

            $defs = null;
            $reps = null;
            $max = 0;
            if ($pageType === PageType::DataPage) {
                $raw = Compression::decompress($codec, $body, $uncompressed);
                $rawLen = \strlen($raw);
                $p = 0;
                if ($maxRep > 0) {
                    $reps = self::levelsV1($raw, $p, $rawLen, $nv, $maxRep, Fields::int($dh, 4, 'DataPageHeader.repetition_level_encoding'), $name, 'repetition');
                }
                if ($maxDef > 0) {
                    $defs = self::levelsV1($raw, $p, $rawLen, $nv, $maxDef, $defEnc, $name, 'definition');
                }
            } else {
                $dl = Fields::int($dh, 5, 'DataPageHeaderV2.definition_levels_byte_length');
                $rl = Fields::int($dh, 6, 'DataPageHeaderV2.repetition_levels_byte_length');
                $isCompressed = Fields::optBool($dh, 7, 'DataPageHeaderV2.is_compressed') ?? true;
                if ($dl < 0 || $rl < 0 || $dl + $rl > $compressed || $dl + $rl > $uncompressed) {
                    throw new ParquetException("Column '$name': level lengths of a page exceed the page");
                }
                $rest = \substr($body, $dl + $rl);
                $values = $isCompressed && $codec !== Codec::Uncompressed
                    ? Compression::decompress($codec, $rest, $uncompressed - $dl - $rl)
                    : ($compressed === $uncompressed ? $rest : throw new ParquetException("Column '$name': an uncompressed page has two different sizes"));
                if ($maxRep > 0) {
                    $reps = RleHybrid::decode($body, 0, $rl, self::bitWidth($maxRep), $nv, $max);
                    if ($max > $maxRep) {
                        throw new ParquetException("Column '$name': a repetition level above the maximum of $maxRep");
                    }
                }
                if ($maxDef > 0 && $dl > 0) {
                    $defs = RleHybrid::decode($body, $rl, $rl + $dl, self::bitWidth($maxDef), $nv, $max);
                } elseif ($maxDef > 0 && $nv > 0) {
                    throw new ParquetException("Column '$name': a page of an optional column has no definition levels");
                }
                $raw = $values;
                $rawLen = \strlen($raw);
                $p = 0;
            }
            if ($defs !== null && $max > $maxDef) {
                throw new ParquetException("Column '$name': a definition level above the maximum of $maxDef");
            }
            if ($defs === null) {
                $present = $nv;
            } elseif ($maxDef === 1) {
                $present = \array_sum($defs);
            } else {
                $present = \array_count_values($defs)[$maxDef] ?? 0;
            }

            if ($enc === Encoding::Plain->value) {
                $vals = PlainDecoder::decode($raw, $p, $type, $present, $leaf->typeLength);
            } elseif ($enc === Encoding::PlainDictionary->value || $enc === Encoding::RleDictionary->value) {
                if ($present === 0) {
                    $vals = [];
                } else {
                    if ($dict === null) {
                        throw new ParquetException("Column '$name': a dictionary-encoded page comes before any dictionary page");
                    }
                    if ($p >= $rawLen) {
                        throw new ParquetException("Column '$name': a dictionary-encoded page has no bit width");
                    }
                    $bw = \ord($raw[$p]);
                    $vals = RleHybrid::decodeDictionary($raw, $p + 1, $rawLen, $bw, $present, $dict);
                }
            } elseif ($enc === Encoding::Rle->value && $type === PhysicalType::Boolean) {
                if ($present === 0) {
                    $vals = [];
                } else {
                    if ($rawLen - $p < 4) {
                        throw new ParquetException("Column '$name': an RLE boolean page is cut off");
                    }
                    $len = \ord($raw[$p]) | (\ord($raw[$p + 1]) << 8) | (\ord($raw[$p + 2]) << 16) | (\ord($raw[$p + 3]) << 24);
                    if ($len < 0 || $len > $rawLen - $p - 4) {
                        throw new ParquetException("Column '$name': an RLE boolean run runs past the end of the page");
                    }
                    $bits = RleHybrid::decode($raw, $p + 4, $p + 4 + $len, 1, $present, $max);
                    $vals = [];
                    foreach ($bits as $b) {
                        $vals[] = $b !== 0;
                    }
                }
            } elseif ($enc === Encoding::DeltaBinaryPacked->value) {
                if ($type !== PhysicalType::Int32 && $type !== PhysicalType::Int64) {
                    throw new ParquetException("Column '$name': DELTA_BINARY_PACKED page in a {$type->name} column");
                }
                $q = $p;
                $vals = Delta::binaryPacked($raw, $q, $rawLen, $type === PhysicalType::Int64, $present);
            } elseif ($enc === Encoding::DeltaLengthByteArray->value) {
                if ($type !== PhysicalType::ByteArray) {
                    throw new ParquetException("Column '$name': DELTA_LENGTH_BYTE_ARRAY page in a {$type->name} column");
                }
                $q = $p;
                $vals = Delta::lengthByteArray($raw, $q, $rawLen, $present);
            } elseif ($enc === Encoding::DeltaByteArray->value) {
                if ($type !== PhysicalType::ByteArray && $type !== PhysicalType::FixedLenByteArray) {
                    throw new ParquetException("Column '$name': DELTA_BYTE_ARRAY page in a {$type->name} column");
                }
                $q = $p;
                $vals = Delta::byteArray($raw, $q, $rawLen, $present, $type === PhysicalType::FixedLenByteArray ? $leaf->typeLength : null);
            } elseif ($enc === Encoding::ByteStreamSplit->value) {
                $vals = ByteStreamSplit::decode($raw, $p, $type, $present, $leaf->typeLength);
            } else {
                throw new UnsupportedException('encoding ' . self::encodingName($enc));
            }

            if (!$merge) {
                $parts[] = $vals;
                if ($defs !== null) {
                    $defParts[] = $defs;
                }
                if ($reps !== null) {
                    $repParts[] = $reps;
                }
                $seen += $nv;
                continue;
            }
            if ($defs === null || $present === $nv) {
                $part = $vals;
            } elseif ($present === 0) {
                $part = \array_fill(0, $nv, null);
            } else {
                $part = [];
                $k = 0;
                foreach ($defs as $d) {
                    $part[] = $d !== 0 ? $vals[$k++] : null;
                }
            }
            $parts[] = $part;
            $seen += $nv;
        }
        if ($partial && $seen > $meta->numValues) {
            throw new ParquetException("Column '$name': the selected pages hold more values than the chunk");
        }
        $join = static fn (array $p): array => \count($p) === 1 ? $p[0] : ($p === [] ? [] : \array_merge(...$p));
        return [$join($parts), $maxDef > 0 && !$merge ? $join($defParts) : null, $maxRep > 0 ? $join($repParts) : null];
    }

    /** Bits needed for levels 0..$max. */
    private static function bitWidth(int $max): int
    {
        $w = 0;
        while ($max > 0) {
            ++$w;
            $max >>= 1;
        }
        return $w;
    }

    /**
     * The levels of a data page v1: RLE with a 4-byte length prefix, or the deprecated BIT_PACKED (most significant bit first, no prefix).
     *
     * @return list<int>
     */
    private static function levelsV1(string $raw, int &$p, int $rawLen, int $nv, int $maxLevel, int $enc, string $name, string $what): array
    {
        $bw = self::bitWidth($maxLevel);
        $max = 0;
        if ($enc === Encoding::Rle->value) {
            if ($rawLen - $p < 4) {
                throw new ParquetException("Column '$name': a page ends inside its $what level length");
            }
            $len = \ord($raw[$p]) | (\ord($raw[$p + 1]) << 8) | (\ord($raw[$p + 2]) << 16) | (\ord($raw[$p + 3]) << 24);
            if ($len < 0 || $len > $rawLen - $p - 4) {
                throw new ParquetException("Column '$name': $what levels run past the end of the page");
            }
            $levels = RleHybrid::decode($raw, $p + 4, $p + 4 + $len, $bw, $nv, $max);
            $p += 4 + $len;
        } elseif ($enc === Encoding::BitPacked->value) {
            $bytes = ($nv * $bw + 7) >> 3;
            if ($bytes > $rawLen - $p) {
                throw new ParquetException("Column '$name': $what levels run past the end of the page");
            }
            $levels = [];
            $bit = 0;
            for ($i = 0; $i < $nv; $i++) {
                $v = 0;
                for ($b = 0; $b < $bw; $b++, $bit++) {
                    $v = ($v << 1) | ((\ord($raw[$p + ($bit >> 3)]) >> (7 - ($bit & 7))) & 1);
                }
                $levels[] = $v;
                if ($v > $max) {
                    $max = $v;
                }
            }
            $p += $bytes;
        } else {
            throw new UnsupportedException("$what level encoding " . self::encodingName($enc));
        }
        if ($max > $maxLevel) {
            throw new ParquetException("Column '$name': a $what level above the maximum of $maxLevel");
        }
        return $levels;
    }

    private static function encodingName(int $id): string
    {
        $e = Encoding::tryFrom($id);
        return $e === null ? (string) $id : \strtoupper(\preg_replace('/(?<!^)(?=[A-Z])/', '_', $e->name) ?? $e->name);
    }
}
