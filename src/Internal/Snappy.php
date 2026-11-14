<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\FormatException;

/** Snappy's raw (unframed) format. The decoding logic is the one measured in the skein-std reader; the checks are added. */
final class Snappy
{
    public static function decode(string $in, int $expected): string
    {
        $len = \strlen($in);
        $p = 0;
        $n = 0;
        for ($shift = 0; ; $shift += 7) {
            if ($p >= $len || $shift > 28) {
                throw new FormatException('Snappy data has no valid length header');
            }
            $b = \ord($in[$p++]);
            $n |= ($b & 0x7F) << $shift;
            if (($b & 0x80) === 0) {
                break;
            }
        }
        if ($n !== $expected) {
            throw new FormatException("Snappy page says it holds $n bytes, the page header says $expected");
        }
        $out = '';
        $outLen = 0;
        while ($p < $len) {
            $t = \ord($in[$p++]);
            switch ($t & 3) {
                case 0:
                    $l = $t >> 2;
                    if ($l >= 60) {
                        $k = $l - 59;
                        if ($p + $k > $len) {
                            throw new FormatException('Snappy literal length is cut off');
                        }
                        $l = 0;
                        for ($i = 0; $i < $k; $i++) {
                            $l |= \ord($in[$p + $i]) << (8 * $i);
                        }
                        $p += $k;
                    }
                    ++$l;
                    if ($p + $l > $len || $outLen + $l > $n) {
                        throw new FormatException('Snappy literal runs past the data or the declared size');
                    }
                    $out .= \substr($in, $p, $l);
                    $p += $l;
                    $outLen += $l;
                    continue 2;
                case 1:
                    if ($p >= $len) {
                        throw new FormatException('Snappy copy is cut off');
                    }
                    $l = (($t >> 2) & 7) + 4;
                    $off = (($t >> 5) << 8) | \ord($in[$p++]);
                    break;
                case 2:
                    if ($p + 2 > $len) {
                        throw new FormatException('Snappy copy is cut off');
                    }
                    $l = ($t >> 2) + 1;
                    $off = \ord($in[$p]) | (\ord($in[$p + 1]) << 8);
                    $p += 2;
                    break;
                default:
                    if ($p + 4 > $len) {
                        throw new FormatException('Snappy copy is cut off');
                    }
                    $l = ($t >> 2) + 1;
                    $off = \ord($in[$p]) | (\ord($in[$p + 1]) << 8) | (\ord($in[$p + 2]) << 16) | (\ord($in[$p + 3]) << 24);
                    $p += 4;
                    break;
            }
            if ($off < 1 || $off > $outLen || $outLen + $l > $n) {
                throw new FormatException('Snappy copy reaches outside the output');
            }
            $start = $outLen - $off;
            if ($off >= $l) {
                $out .= \substr($out, $start, $l);
            } elseif ($off === 1) {
                $out .= \str_repeat($out[$start], $l); // (an overlapping copy: a repeated byte)
            } else {
                $out .= \substr(\str_repeat(\substr($out, $start, $off), \intdiv($l, $off) + 1), 0, $l); // (a repeated pattern)
            }
            $outLen += $l;
        }
        if ($outLen !== $n) {
            throw new FormatException("Snappy data ends after $outLen bytes, its header says $n");
        }
        return $out;
    }
}
