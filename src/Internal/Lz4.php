<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\FormatException;

/** LZ4: the block format (what LZ4_RAW pages hold), Hadoop framing around blocks, and the LZ4 frame format. */
final class Lz4
{
    /**
     * One LZ4 block that must decompress to exactly $expected bytes.
     */
    public static function block(string $in, int $expected): string
    {
        $out = self::blockUpTo($in, $expected, $n);
        if ($n !== $expected) {
            throw new FormatException("LZ4 data ends after $n bytes, expected $expected");
        }
        return $out;
    }

    /**
     * Hadoop's framing: repeated [4-byte big-endian uncompressed size][4-byte big-endian compressed size][block], to exactly the
     * end of the input and to exactly $expected bytes. Returns null if the input is not framed that way.
     */
    public static function hadoop(string $in, int $expected): ?string
    {
        $len = \strlen($in);
        $p = 0;
        $out = '';
        $total = 0;
        while ($p < $len) {
            if ($len - $p < 8) {
                return null;
            }
            $d = (\ord($in[$p]) << 24) | (\ord($in[$p + 1]) << 16) | (\ord($in[$p + 2]) << 8) | \ord($in[$p + 3]);
            $c = (\ord($in[$p + 4]) << 24) | (\ord($in[$p + 5]) << 16) | (\ord($in[$p + 6]) << 8) | \ord($in[$p + 7]);
            $p += 8;
            if ($c > $len - $p || $d > $expected - $total) {
                return null;
            }
            try {
                $out .= self::block(\substr($in, $p, $c), $d);
            } catch (FormatException) {
                return null;
            }
            $p += $c;
            $total += $d;
        }
        return $total === $expected ? $out : null;
    }

    /**
     * An LZ4 frame (magic 0x184D2204), which must decompress to exactly $expected bytes. Checksums are not verified.
     * Returns null if the input does not start with the frame magic.
     */
    public static function frame(string $in, int $expected): ?string
    {
        $len = \strlen($in);
        if ($len < 7 || \substr($in, 0, 4) !== "\x04\x22\x4D\x18") {
            return null;
        }
        $flg = \ord($in[4]);
        if (($flg >> 6) !== 1) {
            throw new FormatException('LZ4 frame version is not 1');
        }
        $blockChecksum = ($flg >> 4) & 1;
        $contentSize = ($flg >> 3) & 1;
        $contentChecksum = ($flg >> 2) & 1;
        $dictId = $flg & 1;
        $p = 6 + ($contentSize ? 8 : 0) + ($dictId ? 4 : 0) + 1; // FLG, BD, [size], [dict id], header checksum
        $out = '';
        $total = 0;
        while (true) {
            if ($len - $p < 4) {
                throw new FormatException('LZ4 frame block header is cut off');
            }
            $bs = \ord($in[$p]) | (\ord($in[$p + 1]) << 8) | (\ord($in[$p + 2]) << 16) | (\ord($in[$p + 3]) << 24);
            $p += 4;
            if ($bs === 0) {
                break;
            }
            $stored = ($bs & 0x80000000) !== 0;
            $bs &= 0x7FFFFFFF;
            if ($bs > $len - $p) {
                throw new FormatException('LZ4 frame block is cut off');
            }
            $data = \substr($in, $p, $bs);
            $p += $bs + ($blockChecksum ? 4 : 0);
            if ($stored) {
                if (\strlen($data) > $expected - $total) {
                    throw new FormatException('LZ4 frame is larger than expected');
                }
                $out .= $data;
                $total += \strlen($data);
            } else {
                // block size limit of the frame: at most 4 MiB; decompress against what is left of the expected size
                $out .= self::blockUpTo($data, $expected - $total, $n);
                $total += $n;
            }
        }
        if ($total !== $expected) {
            throw new FormatException("LZ4 frame holds $total bytes, expected $expected");
        }
        return $out;
    }

    /** A block of at most $max bytes of output: its size is not known in advance (in a frame), only an upper bound. */
    private static function blockUpTo(string $in, int $max, ?int &$produced): string
    {
        $len = \strlen($in);
        $p = 0;
        $out = '';
        $outLen = 0;
        while ($p < $len) {
            $token = \ord($in[$p++]);
            $lit = $token >> 4;
            if ($lit === 15) {
                do {
                    if ($p >= $len) {
                        throw new FormatException('LZ4 literal length is cut off');
                    }
                    $b = \ord($in[$p++]);
                    $lit += $b;
                } while ($b === 255);
            }
            if ($lit > $len - $p || $lit > $max - $outLen) {
                throw new FormatException('LZ4 literals run past the data or the expected size');
            }
            $out .= \substr($in, $p, $lit);
            $p += $lit;
            $outLen += $lit;
            if ($p >= $len) {
                break;
            }
            if ($len - $p < 2) {
                throw new FormatException('LZ4 match offset is cut off');
            }
            $off = \ord($in[$p]) | (\ord($in[$p + 1]) << 8);
            $p += 2;
            $ml = $token & 15;
            if ($ml === 15) {
                do {
                    if ($p >= $len) {
                        throw new FormatException('LZ4 match length is cut off');
                    }
                    $b = \ord($in[$p++]);
                    $ml += $b;
                } while ($b === 255);
            }
            $ml += 4;
            if ($off < 1 || $off > $outLen || $ml > $max - $outLen) {
                throw new FormatException('LZ4 match reaches outside the output');
            }
            $start = $outLen - $off;
            $out .= $off >= $ml ? \substr($out, $start, $ml) : \substr(\str_repeat(\substr($out, $start, $off), \intdiv($ml, $off) + 1), 0, $ml);
            $outLen += $ml;
        }
        $produced = $outLen;
        return $out;
    }
}
