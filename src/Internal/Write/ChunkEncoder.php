<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

use Charm\Parquet\{Codec, Encoding, PageType, PhysicalType};
use Charm\Parquet\Thrift\CompactWriter as T;

/**
 * Encodes one column chunk (all the buffered entries of a leaf) into pages: an optional dictionary page, then data pages that each start
 * a record, with statistics and the page index entries.
 */
final class ChunkEncoder
{
    /**
     * @return array{
     *   bytes: string, encodings: list<int>, numValues: int, uncompressed: int, compressed: int, dataOffset: int, dictOffset: ?int,
     *   stats: array{mixed, mixed, int}|null, nullCount: int,
     *   pages: list<array{int, int, int}>, columnIndex: ?string
     * } offsets are absolute: the chunk starts at $start
     */
    public static function encode(ChunkBuffer $b, WNode $leaf, WriteOptions $o, int $start): array
    {
        $spec = $leaf->spec;
        $phys = $spec->phys;
        $maxDef = $leaf->maxDef;
        $maxRep = $leaf->maxRep;
        $vals = $b->vals;
        $defs = $maxDef > 0 ? $b->defs : null;
        $reps = $maxRep > 0 ? $b->reps : null;
        $n = $defs !== null ? \count($defs) : \count($vals);
        $defBits = self::bitWidth($maxDef);
        $repBits = self::bitWidth($maxRep);

        // pages: whole records, about $dataPageSize of PLAIN bytes each
        $perEntry = ($b->plainBytes + $spec->width * \count($vals) + $n) / max(1, $n);
        $entriesPerPage = \max(1, (int) ($o->dataPageSize / \max(0.125, $perEntry)));
        $starts = $reps !== null ? \array_keys($reps, 0, true) : null;
        $records = $starts !== null ? \count($starts) : $n;
        $perPage = \max(1, (int) ($entriesPerPage * $records / \max(1, $n)));
        $bounds = [];
        for ($r = 0; $r < $records; $r += $perPage) {
            $bounds[] = [$starts !== null ? $starts[$r] : $r, $r];
        }

        $dictOn = $o->dictionary && $spec->dictionary && $vals !== [];
        $dict = [];      // value => id
        $dictList = [];  // id => value
        $dictBytes = 0;
        $dictWidth = $spec->width;
        $pages = [];
        $encodingsUsed = [];
        $vi = 0;
        $pageInfo = [];

        foreach ($bounds as $k => [$from, $recFrom]) {
            $to = isset($bounds[$k + 1]) ? $bounds[$k + 1][0] : $n;
            $recTo = isset($bounds[$k + 1]) ? $bounds[$k + 1][1] : $records;
            $pd = $defs !== null ? \array_slice($defs, $from, $to - $from) : null;
            $pr = $reps !== null ? \array_slice($reps, $from, $to - $from) : null;
            $present = $pd !== null ? ($maxDef === 1 ? \array_sum($pd) : (\array_count_values($pd)[$maxDef] ?? 0)) : $to - $from;
            $pv = $present === 0 ? [] : \array_slice($vals, $vi, $present);
            $vi += $present;

            $ids = null;
            if ($dictOn && $pv !== []) {
                $added = [];
                $ids = [];
                $newBytes = 0;
                foreach ($pv as $v) {
                    $id = $dict[$v] ?? null;
                    if ($id === null) {
                        $id = \count($dictList);
                        $dict[$v] = $id;
                        $dictList[] = $v;
                        $added[] = $v;
                        $newBytes += $dictWidth !== 0 ? $dictWidth : 4 + \strlen($v);
                    }
                    $ids[] = $id;
                }
                if ($dictBytes + $newBytes > $o->dictionaryPageSize) {
                    // the dictionary is full: this page and the rest are PLAIN (what the dictionary holds before this page stays)
                    foreach ($added as $v) {
                        unset($dict[$v]);
                    }
                    \array_splice($dictList, \count($dictList) - \count($added));
                    $ids = null;
                    $dictOn = false;
                } else {
                    $dictBytes += $newBytes;
                }
            }
            $pageInfo[] = [$pd, $pr, $pv, $ids, \count($dictList), $recTo - $recFrom, $from, $to, $recFrom];
        }
        $dictUsed = $dictList !== [] && \array_filter($pageInfo, static fn (array $p): bool => $p[3] !== null) !== [];

        $out = '';
        $uncompressed = 0;
        $dictOffset = null;
        $encodings = [];
        if ($dictUsed) {
            $dictOffset = $start;
            $body = Plain::encode($dictList, $phys);
            $comp = $o->compress($body);
            $header = T::struct([
                [1, T::I32, PageType::DictionaryPage->value],
                [2, T::I32, \strlen($body)],
                [3, T::I32, \strlen($comp)],
                [7, T::STRUCT, [[1, T::I32, \count($dictList)], [2, T::I32, Encoding::Plain->value]]],
            ]);
            $out .= $header . $comp;
            $uncompressed += \strlen($header) + \strlen($body);
            $encodings[Encoding::Plain->value] = true;
        }
        $dataOffset = $start + \strlen($out);

        $statKind = $o->statistics ? $spec->stat : 'none';
        $chunkMin = $chunkMax = null;
        $chunkHas = false;
        $nullTotal = 0;
        $pageLocs = [];
        $ciNull = [];
        $ciMin = [];
        $ciMax = [];
        $ciNulls = [];
        $ciComplete = $statKind !== 'none';
        $pageMm = [];
        foreach ($pageInfo as [$pd, $pr, $pv, $ids, $dictSize, $nRecords, $from, $to, $recFrom]) {
            $nEntries = $to - $from;
            $nulls = $nEntries - \count($pv);
            $nullTotal += $nulls;
            $levels = [];
            $repEnc = $pr !== null ? RleEncoder::encode($pr, $repBits) : '';
            $defEnc = $pd !== null ? RleEncoder::encode($pd, $defBits) : '';
            if ($ids !== null) {
                $bw = \max(1, self::bitWidth($dictSize - 1));
                $valEnc = \chr($bw) . RleEncoder::encode($ids, $bw);
                $enc = Encoding::RleDictionary->value;
            } else {
                $valEnc = Plain::encode($pv, $phys);
                $enc = Encoding::Plain->value;
            }
            $encodings[$enc] = true;
            if ($pd !== null || $pr !== null) {
                $encodings[Encoding::Rle->value] = true;
            }
            if ($o->pageVersion === 2) {
                $comp = $valEnc === '' ? '' : $o->compress($valEnc);
                $compressed = $o->codec !== Codec::Uncompressed;
                $uncompressedSize = \strlen($repEnc) + \strlen($defEnc) + \strlen($valEnc);
                $header = T::struct([
                    [1, T::I32, PageType::DataPageV2->value],
                    [2, T::I32, $uncompressedSize],
                    [3, T::I32, \strlen($repEnc) + \strlen($defEnc) + \strlen($comp)],
                    [8, T::STRUCT, [[1, T::I32, $nEntries], [2, T::I32, $nulls], [3, T::I32, $nRecords], [4, T::I32, $enc],
                        [5, T::I32, \strlen($defEnc)], [6, T::I32, \strlen($repEnc)], [7, T::BOOL, $compressed]]],
                ]);
                $page = $header . $repEnc . $defEnc . $comp;
            } else {
                $body = ($pr !== null ? \pack('V', \strlen($repEnc)) . $repEnc : '') . ($pd !== null ? \pack('V', \strlen($defEnc)) . $defEnc : '') . $valEnc;
                $comp = $o->compress($body);
                $uncompressedSize = \strlen($body);
                $header = T::struct([
                    [1, T::I32, PageType::DataPage->value],
                    [2, T::I32, $uncompressedSize],
                    [3, T::I32, \strlen($comp)],
                    [5, T::STRUCT, [[1, T::I32, $nEntries], [2, T::I32, $enc], [3, T::I32, Encoding::Rle->value], [4, T::I32, Encoding::Rle->value]]],
                ]);
                $page = $header . $comp;
            }
            $pageLocs[] = [$start + \strlen($out), \strlen($page), $recFrom];
            $out .= $page;
            $uncompressed += \strlen($header) + $uncompressedSize;

            $mm = $statKind === 'none' ? null : Stats::minMax($pv, $statKind);
            if ($mm !== null) {
                if (!$chunkHas) {
                    [$chunkMin, $chunkMax] = $mm;
                    $chunkHas = true;
                } else {
                    if (Stats::compare($mm[0], $chunkMin, $statKind) < 0) {
                        $chunkMin = $mm[0];
                    }
                    if (Stats::compare($mm[1], $chunkMax, $statKind) > 0) {
                        $chunkMax = $mm[1];
                    }
                }
            }
            $isNullPage = $pv === [];
            $ciNull[] = $isNullPage;
            $ciNulls[] = $nulls;
            if ($mm === null && !$isNullPage) {
                $ciComplete = false; // (a page of only NaN, or of values too long to use: no column index for this chunk)
            }
            $ciMin[] = $mm === null ? '' : Stats::raw($mm[0], $phys);
            $ciMax[] = $mm === null ? '' : Stats::raw($mm[1], $phys);
            if ($mm !== null) {
                $pageMm[] = $mm;
            }
        }

        $columnIndex = null;
        if ($o->pageIndex && $o->statistics && $ciComplete && $statKind !== 'none') {
            $order = 0;
            if (\count($pageMm) > 1) {
                $asc = $desc = true;
                for ($i = 1, $m = \count($pageMm); $i < $m; $i++) {
                    $a = Stats::compare($pageMm[$i][0], $pageMm[$i - 1][0], $statKind);
                    $z = Stats::compare($pageMm[$i][1], $pageMm[$i - 1][1], $statKind);
                    $asc = $asc && $a >= 0 && $z >= 0;
                    $desc = $desc && $a <= 0 && $z <= 0;
                }
                $order = $asc ? 1 : ($desc ? 2 : 0);
            }
            $columnIndex = T::struct([
                [1, T::LIST, [T::BOOL, $ciNull]],
                [2, T::LIST, [T::BINARY, $ciMin]],
                [3, T::LIST, [T::BINARY, $ciMax]],
                [4, T::I32, $order],
                [5, T::LIST, [T::I64, $ciNulls]],
            ]);
        }
        \ksort($encodings);
        return [
            'bytes' => $out,
            'encodings' => \array_keys($encodings),
            'numValues' => $n,
            'uncompressed' => $uncompressed,
            'compressed' => \strlen($out),
            'dataOffset' => $dataOffset,
            'dictOffset' => $dictOffset,
            'stats' => $chunkHas ? [$chunkMin, $chunkMax, $nullTotal] : null,
            'nullCount' => $nullTotal,
            'pages' => $pageLocs,
            'columnIndex' => $columnIndex,
        ];
    }

    public static function bitWidth(int $max): int
    {
        $w = 0;
        while ($max > 0) {
            ++$w;
            $max >>= 1;
        }
        return $w;
    }
}
