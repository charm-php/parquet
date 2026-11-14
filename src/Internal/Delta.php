<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\FormatException;

/** DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY. */
final class Delta
{
    /**
     * DELTA_BINARY_PACKED: exactly $count values from $raw[$p .. $end); $p is moved past the stream (the position after the last
     * miniblock that holds values: miniblocks that are not needed take no space).
     *
     * Arithmetic wraps: INT64 modulo 2^64 (PHP ints would turn into floats on overflow), INT32 modulo 2^32.
     *
     * @return list<int>
     */
    public static function binaryPacked(string $raw, int &$p, int $end, bool $is64, int $count): array
    {
        if ($end > \strlen($raw) || $p > $end) {
            throw new FormatException('DELTA_BINARY_PACKED data lies outside the page');
        }
        $blockSize = self::uvarint($raw, $p, $end);
        $miniblocks = self::uvarint($raw, $p, $end);
        $total = self::uvarint($raw, $p, $end);
        $first = self::uvarint($raw, $p, $end);
        $first = (($first >> 1) & \PHP_INT_MAX) ^ -($first & 1);
        if ($total !== $count) {
            throw new FormatException("DELTA_BINARY_PACKED stream holds $total values, the page needs $count");
        }
        if ($count === 0) {
            return [];
        }
        if ($blockSize <= 0 || $blockSize % 128 !== 0 || $blockSize > 1 << 20 || $miniblocks <= 0 || $miniblocks > $blockSize || $blockSize % $miniblocks !== 0) {
            throw new FormatException("DELTA_BINARY_PACKED block size $blockSize with $miniblocks miniblocks is not valid");
        }
        $perMini = \intdiv($blockSize, $miniblocks);
        if ($perMini % 8 !== 0) {
            throw new FormatException('DELTA_BINARY_PACKED miniblocks must hold a multiple of 8 values');
        }
        if (!$is64) {
            $first = (($first & 0xFFFFFFFF) ^ 0x80000000) - 0x80000000; // (not + 0x80000000: that overflows into a float for large values)
        }
        $out = [$first];
        $v = $first;
        $left = $count - 1;
        while ($left > 0) {
            $minDelta = self::uvarint($raw, $p, $end);
            $minDelta = (($minDelta >> 1) & \PHP_INT_MAX) ^ -($minDelta & 1);
            if ($miniblocks > $end - $p) {
                throw new FormatException('DELTA_BINARY_PACKED bit widths are cut off');
            }
            $widthsAt = $p;
            $p += $miniblocks;
            $md32 = $minDelta & 0xFFFFFFFF;
            for ($m = 0; $m < $miniblocks && $left > 0; $m++) {
                $bw = \ord($raw[$widthsAt + $m]);
                if ($bw > 64) {
                    throw new FormatException("DELTA_BINARY_PACKED bit width $bw");
                }
                $take = \min($perMini, $left);
                $full = $perMini * $bw >> 3; // (a miniblock is padded to its full size)
                $need = ($take * $bw + 7) >> 3;
                if ($need > $end - $p) {
                    throw new FormatException('DELTA_BINARY_PACKED miniblock is cut off');
                }
                $q = $p;
                $p += \min($full, $end - $p);
                if ($bw === 0) {
                    // every delta is the minimum delta
                    if ($is64) {
                        for ($i = 0; $i < $take; $i++) {
                            $s = $v + $minDelta;
                            $v = \is_int($s) ? $s : self::wrapAdd($v, $minDelta);
                            $out[] = $v;
                        }
                    } else {
                        for ($i = 0; $i < $take; $i++) {
                            $v = ($v + $md32) & 0xFFFFFFFF;
                            $out[] = $v >= 0x80000000 ? $v - 0x100000000 : $v;
                        }
                        $v = $out[\count($out) - 1];
                    }
                } elseif ($bw <= 56) {
                    $mask = (1 << $bw) - 1;
                    $acc = 0;
                    $bits = 0;
                    if ($is64) {
                        for ($i = 0; $i < $take; $i++) {
                            while ($bits < $bw) {
                                $acc |= \ord($raw[$q++]) << $bits;
                                $bits += 8;
                            }
                            $d = $acc & $mask;
                            $acc >>= $bw;
                            $bits -= $bw;
                            // $d + $minDelta + $v, wrapping
                            $s = $d + $minDelta;
                            if (!\is_int($s)) {
                                $s = self::wrapAdd($d, $minDelta);
                            }
                            $t = $v + $s;
                            $v = \is_int($t) ? $t : self::wrapAdd($v, $s);
                            $out[] = $v;
                        }
                    } else {
                        $v &= 0xFFFFFFFF;
                        for ($i = 0; $i < $take; $i++) {
                            while ($bits < $bw) {
                                $acc |= \ord($raw[$q++]) << $bits;
                                $bits += 8;
                            }
                            $d = $acc & $mask;
                            $acc >>= $bw;
                            $bits -= $bw;
                            $v = ($v + ($d & 0xFFFFFFFF) + $md32) & 0xFFFFFFFF;
                            $out[] = $v >= 0x80000000 ? $v - 0x100000000 : $v;
                        }
                        $v = $out[\count($out) - 1];
                    }
                } else {
                    // 57..64 bits: a value spans up to 9 bytes; assemble it from bytes
                    $bitPos = 0;
                    $buf = \substr($raw, $q, $need) . "\0\0\0\0\0\0\0\0\0"; // (zero padding: a value may be read with up to 9 bytes)
                    for ($i = 0; $i < $take; $i++) {
                        $byte = $bitPos >> 3;
                        $shift = $bitPos & 7;
                        $d = 0;
                        for ($k = 0; $k < 8; $k++) {
                            $d |= \ord($buf[$byte + $k]) << (8 * $k);
                        }
                        if ($shift !== 0) {
                            $d = ($d >> $shift) & (\PHP_INT_MAX >> ($shift - 1));
                            if ($bw + $shift > 64) {
                                $d |= \ord($buf[$byte + 8]) << (64 - $shift);
                            }
                        }
                        if ($bw < 64) {
                            $d &= $bw === 63 ? \PHP_INT_MAX : (1 << $bw) - 1; // (1 << 63) - 1 would overflow into a float
                        }
                        $bitPos += $bw;
                        $s = $d + $minDelta;
                        if (!\is_int($s)) {
                            $s = self::wrapAdd($d, $minDelta);
                        }
                        $t = $v + $s;
                        $v = \is_int($t) ? $t : self::wrapAdd($v, $s);
                        if (!$is64) {
                            $v = (($v & 0xFFFFFFFF) ^ 0x80000000) - 0x80000000;
                        }
                        $out[] = $v;
                    }
                }
                $left -= $take;
            }
        }
        return $out;
    }

    /** DELTA_LENGTH_BYTE_ARRAY: the lengths as a delta stream, then all the bytes. @return list<string> */
    public static function lengthByteArray(string $raw, int &$p, int $end, int $count): array
    {
        $lengths = self::binaryPacked($raw, $p, $end, false, $count);
        $out = [];
        $sum = 0;
        foreach ($lengths as $l) {
            if ($l < 0) {
                throw new FormatException('DELTA_LENGTH_BYTE_ARRAY has a negative length');
            }
            $sum += $l;
        }
        if ($sum > $end - $p) {
            throw new FormatException("DELTA_LENGTH_BYTE_ARRAY values need $sum bytes, the page has " . ($end - $p));
        }
        foreach ($lengths as $l) {
            $out[] = \substr($raw, $p, $l);
            $p += $l;
        }
        return $out;
    }

    /**
     * DELTA_BYTE_ARRAY: prefix lengths (delta stream), then the suffixes (DELTA_LENGTH_BYTE_ARRAY); each value is the first
     * prefix-length bytes of the previous value followed by its suffix. For FIXED_LEN_BYTE_ARRAY pass $fixedLength.
     *
     * @return list<string>
     */
    public static function byteArray(string $raw, int &$p, int $end, int $count, ?int $fixedLength): array
    {
        $prefixes = self::binaryPacked($raw, $p, $end, false, $count);
        $suffixes = self::lengthByteArray($raw, $p, $end, $count);
        $out = [];
        $prev = '';
        $prevLen = 0;
        foreach ($prefixes as $i => $pre) {
            if ($pre < 0 || $pre > $prevLen) {
                throw new FormatException('DELTA_BYTE_ARRAY prefix length ' . $pre . ' is longer than the previous value');
            }
            $prev = ($pre === 0 ? '' : \substr($prev, 0, $pre)) . $suffixes[$i];
            $prevLen = \strlen($prev);
            if ($fixedLength !== null && $prevLen !== $fixedLength) {
                throw new FormatException("DELTA_BYTE_ARRAY value of $prevLen bytes in a FIXED_LEN_BYTE_ARRAY($fixedLength) column");
            }
            $out[] = $prev;
        }
        return $out;
    }

    /** The sum of two ints modulo 2^64 for the case where PHP's + produced a float. */
    private static function wrapAdd(int $a, int $b): int
    {
        if ($a > 0 && $b > 0) {
            return ($a + \PHP_INT_MIN) + ($b + \PHP_INT_MIN);
        }
        return ($a - \PHP_INT_MIN) + ($b - \PHP_INT_MIN);
    }

    private static function uvarint(string $raw, int &$p, int $end): int
    {
        $v = 0;
        for ($shift = 0; $shift < 70; $shift += 7) {
            if ($p >= $end) {
                throw new FormatException('DELTA_BINARY_PACKED header is cut off');
            }
            $b = \ord($raw[$p++]);
            $v |= ($b & 0x7F) << $shift;
            if (($b & 0x80) === 0) {
                return $v;
            }
        }
        throw new FormatException('DELTA_BINARY_PACKED varint is too long');
    }
}
