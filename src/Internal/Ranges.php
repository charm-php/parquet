<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

/** Row ranges: lists of [first, end) pairs, sorted and non-overlapping. */
final class Ranges
{
    /**
     * Turns user input ([first, count] pairs, in any order, possibly overlapping) into sorted, merged [first, end) pairs.
     *
     * @param list<array{int, int}> $ranges
     * @return list<array{int, int}>
     */
    public static function fromCounts(array $ranges): array
    {
        $out = [];
        foreach ($ranges as $r) {
            if (!\is_array($r) || \count($r) !== 2 || !\is_int($r[0] ?? null) || !\is_int($r[1] ?? null) || $r[0] < 0 || $r[1] < 0) {
                throw new \ValueError('A row range is [first row, row count] with two non-negative ints');
            }
            if ($r[1] > 0) {
                $out[] = [$r[0], $r[0] + $r[1]];
            }
        }
        return self::merge($out);
    }

    /** @param list<array{int, int}> $r @return list<array{int, int}> */
    public static function merge(array $r): array
    {
        \usort($r, static fn (array $a, array $b): int => $a[0] <=> $b[0]);
        $out = [];
        foreach ($r as [$s, $e]) {
            $n = \count($out);
            if ($n > 0 && $s <= $out[$n - 1][1]) {
                if ($e > $out[$n - 1][1]) {
                    $out[$n - 1][1] = $e;
                }
            } else {
                $out[] = [$s, $e];
            }
        }
        return $out;
    }

    /**
     * The part of $ranges inside [$lo, $hi), relative to $lo.
     *
     * @param list<array{int, int}> $ranges
     * @return list<array{int, int}>
     */
    public static function within(array $ranges, int $lo, int $hi): array
    {
        $out = [];
        foreach ($ranges as [$s, $e]) {
            $s = \max($s, $lo);
            $e = \min($e, $hi);
            if ($s < $e) {
                $out[] = [$s - $lo, $e - $lo];
            }
        }
        return $out;
    }
}
