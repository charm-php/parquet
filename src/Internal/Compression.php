<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\{Codec, FormatException, UnsupportedException};
use Charm\Brotli\{Brotli, DecodeException as BrotliDecodeException};
use Charm\Zstd\{DecodeException, Zstd};

/** Page decompression: the result must have exactly the size the page header promises. */
final class Compression
{
    /** Throws UnsupportedException if pages of this codec cannot be decompressed here (LZO; GZIP without ext-zlib). */
    public static function assertSupported(Codec $codec): void
    {
        if ($codec === Codec::Lzo) {
            throw new UnsupportedException('codec LZO');
        }
        if ($codec === Codec::Gzip && !\function_exists('gzdecode')) {
            throw new UnsupportedException('codec GZIP (needs ext-zlib)');
        }
    }

    public static function decompress(Codec $codec, string $body, int $expected): string
    {
        if ($expected === 0 && $body === '') {
            return ''; // (writers emit empty pages as zero bytes whatever the codec)
        }
        switch ($codec) {
            case Codec::Uncompressed:
                $out = $body;
                break;
            case Codec::Snappy:
                return Snappy::decode($body, $expected); // (checks its own size)
            case Codec::Gzip:
                if (!\function_exists('gzdecode')) {
                    throw new UnsupportedException('codec GZIP (needs ext-zlib)');
                }
                // The zlib functions report corrupt data with a warning and false. The warning is swallowed around these calls only
                // (an @ would still call the caller's error handler, which may turn warnings into exceptions).
                $out = self::quietly(static fn () => \gzdecode($body, $expected + 1));
                if ($out === false) {
                    throw new FormatException('A GZIP page does not decompress (or is larger than its header says)');
                }
                if (\strlen($out) < $expected) {
                    $out = self::gzipMembers($body, $expected); // (gzdecode() stops after the first member)
                }
                break;
            case Codec::Lz4Raw:
                return Lz4::block($body, $expected); // (checks its own size)
            case Codec::Lz4:
                // The deprecated LZ4 codec: Hadoop framing, or a plain block, or an LZ4 frame, tried in that order (as Arrow does)
                $out = Lz4::hadoop($body, $expected);
                if ($out === null) {
                    try {
                        return Lz4::block($body, $expected);
                    } catch (FormatException $e) {
                        $out = Lz4::frame($body, $expected) ?? throw $e;
                    }
                }
                break;
            case Codec::Brotli:
                if (\function_exists('brotli_uncompress')) {
                    // ext-brotli, when it is loaded: much faster than the pure-PHP decoder
                    $out = self::quietly(static fn () => \brotli_uncompress($body, $expected + 1));
                    if ($out === false) {
                        throw new FormatException('A BROTLI page does not decompress (or is larger than its header says)');
                    }
                    break;
                }
                try {
                    $out = Brotli::decompress($body, $expected);
                } catch (BrotliDecodeException $e) {
                    throw new FormatException('A BROTLI page does not decompress: ' . $e->getMessage(), 0, $e);
                }
                break;
            case Codec::Zstd:
                try {
                    $out = Zstd::decompress($body, $expected);
                } catch (DecodeException $e) {
                    throw new FormatException('A ZSTD page does not decompress: ' . $e->getMessage(), 0, $e);
                }
                break;
            default:
                throw new UnsupportedException('codec ' . \strtoupper($codec->name)); // (LZO)
        }
        if (\strlen($out) !== $expected) {
            throw new FormatException('A page is ' . \strlen($out) . " bytes after decompression, its header says $expected");
        }
        return $out;
    }

    /** Calls a zlib function with warnings swallowed: it returns false on failure, and that is what the caller checks. */
    private static function quietly(\Closure $fn): mixed
    {
        \set_error_handler(static fn (): bool => true);
        try {
            return $fn();
        } finally {
            \restore_error_handler();
        }
    }

    /** Several gzip members back to back (some writers produce them): inflate them one after the other. */
    private static function gzipMembers(string $body, int $expected): string
    {
        if (!\function_exists('inflate_init')) {
            throw new UnsupportedException('codec GZIP with several members (needs ext-zlib)');
        }
        $out = '';
        $pos = 0;
        $len = \strlen($body);
        while ($pos < $len) {
            $ctx = self::quietly(static fn () => \inflate_init(\ZLIB_ENCODING_GZIP));
            if ($ctx === false) {
                throw new FormatException('A GZIP page cannot be inflated');
            }
            $off = $pos;
            while (true) {
                $piece = \substr($body, $off, 16384);
                if ($piece === '') {
                    throw new FormatException('A GZIP member is cut off');
                }
                $d = self::quietly(static fn () => \inflate_add($ctx, $piece, \ZLIB_SYNC_FLUSH));
                if ($d === false) {
                    throw new FormatException('A GZIP page does not decompress');
                }
                $out .= $d;
                if (\strlen($out) > $expected) {
                    throw new FormatException('A GZIP page is larger than its header says');
                }
                if (\inflate_get_status($ctx) === \ZLIB_STREAM_END) {
                    $pos += \inflate_get_read_len($ctx);
                    break;
                }
                $off += \strlen($piece);
            }
        }
        return $out;
    }
}
