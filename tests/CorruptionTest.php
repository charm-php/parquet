<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{ParquetException, Reader, ValueMode};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Truncations and bit flips of real files (fixed seeds). The only allowed outcomes are a ParquetException or a successful read;
 * a PHP warning or notice, a TypeError, or any other Throwable fails the test.
 */
final class CorruptionTest extends TestCase
{
    protected function setUp(): void
    {
        \set_error_handler(static function (int $no, string $msg, string $file, int $line): never {
            throw new \ErrorException($msg, 0, $no, $file, $line);
        });
    }

    protected function tearDown(): void
    {
        \restore_error_handler();
    }

    /** Read everything the library can read; returns true if it got through. */
    private function readAll(string $bytes): bool
    {
        try {
            $r = Reader::fromString($bytes, ValueMode::Logical);
            foreach ($r->schema()->leaves as $leaf) {
                for ($g = 0; $g < $r->rowGroupCount(); $g++) {
                    try {
                        $r->readColumn($g, $leaf->dottedPath);
                    } catch (\Charm\Parquet\UnsupportedException) {
                        // (nested columns: not the point here)
                    }
                }
            }
            foreach ($r->rows() as $_) {
                // (the assembled form of every top-level field, nested ones included)
            }
            return true;
        } catch (ParquetException) {
            return false;
        }
    }

    /** @return array<string, array{string, int, int}> */
    public static function files(): array
    {
        return [
            'zstd, data page v2, no dictionary' => ['null-patterns-v2', 1, 4000],
            'plain, uncompressed' => ['null-patterns', 2, 1500],
            'snappy, v1, dictionary' => ['matrix-snappy-v1-dict', 3, 1200],
            'gzip, v2, dictionary' => ['matrix-gzip-v2-dict', 4, 800],
            'zstd, v1, plain' => ['matrix-zstd-v1-plain', 5, 800],
            'dictionary fallback' => ['dict-fallback-v2', 6, 800],
            'RLE booleans' => ['rle-boolean', 7, 1500],
            'nested schema' => ['nested', 8, 3000],
            'int96 and decimals' => ['logical', 9, 1500],
            'DELTA_BINARY_PACKED' => ['delta-int-n129-v2', 10, 2500],
            'DELTA_BYTE_ARRAY' => ['delta-byte-array-v1', 11, 1500],
            'DELTA_LENGTH_BYTE_ARRAY' => ['delta-length-byte-array-v2', 12, 1500],
            'BYTE_STREAM_SPLIT' => ['byte-stream-split-zstd', 13, 800],
            'LZ4_RAW' => ['lz4raw-v1', 14, 400],
            'logical decimals' => ['decimals', 15, 2000],
            'nested: list of optional ints (v1, tiny pages)' => ['nest-list-v1', 20, 1500],
            'nested: struct, null structs (v2)' => ['nest-struct-v2', 21, 1200],
            'nested: list of structs of lists' => ['nest-list-struct-v1', 22, 1500],
            'nested: maps (v2)' => ['nest-map-v2', 23, 1500],
            'nested: depth six' => ['nest-deep-v1', 24, 1500],
            'logical dates and times' => ['logical-edges', 16, 2500],
        ];
    }

    #[DataProvider('files')]
    public function testBitFlipsAndTruncations(string $name, int $seed, int $iterations): void
    {
        $good = Fixtures::bytes($name);
        $this->assertTrue($this->readAll($good), 'the undamaged file reads');
        \mt_srand($seed);
        $len = \strlen($good);
        $refused = 0;
        for ($i = 0; $i < $iterations; $i++) {
            $c = $good;
            if ($i % 5 === 0) {
                $c = \substr($c, 0, \mt_rand(0, $len - 1));
            } elseif ($i % 5 === 1) {
                // damage near the end, where the footer is
                $p = \mt_rand(\max(0, $len - 400), $len - 1);
                $c[$p] = \chr(\ord($c[$p]) ^ (1 << \mt_rand(0, 7)));
            } else {
                for ($k = \mt_rand(1, 3); $k > 0; $k--) {
                    $p = \mt_rand(0, $len - 1);
                    $c[$p] = \chr(\ord($c[$p]) ^ (1 << \mt_rand(0, 7)));
                }
            }
            if (!$this->readAll($c)) {
                ++$refused;
            }
        }
        $this->assertGreaterThan(0, $refused, 'some damage must be noticed');
        $this->addToAssertionCount($iterations);
    }

    public function testEveryTruncationOfASmallFile(): void
    {
        $good = Fixtures::bytes('null-patterns-v2');
        for ($n = 0; $n < \strlen($good); $n++) {
            $this->assertFalse($this->readAll(\substr($good, 0, $n)), "cut to $n bytes");
        }
    }

    public function testEveryByteReplacedInTheFooterOfASmallFile(): void
    {
        $good = Fixtures::bytes('null-patterns-v2');
        $len = \strlen($good);
        for ($p = \max(0, $len - 600); $p < $len; $p++) {
            foreach ([0x00, 0xFF, 0x80] as $b) {
                $c = $good;
                $c[$p] = \chr($b);
                $this->readAll($c);
                $this->addToAssertionCount(1);
            }
        }
    }
}
