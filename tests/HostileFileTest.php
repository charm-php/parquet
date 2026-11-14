<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Codec, ParquetException, Reader, UnsupportedException};
use Charm\Zstd\Zstd;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Files built by hand with sizes, counts and offsets that lie: each must end in a ParquetException (or UnsupportedException), cheaply. */
final class HostileFileTest extends TestCase
{
    private const INT32 = 1;
    private const BYTE_ARRAY = 6;

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

    private static function ints(int ...$v): string
    {
        return \pack('V*', ...$v);
    }

    /** @return array{name: string, type: int, optional?: bool, chunk: string} */
    private static function col(string $chunk, int $type = self::INT32, bool $optional = false, array $extra = []): array
    {
        return ['name' => 'c', 'type' => $type, 'optional' => $optional, 'chunk' => $chunk] + $extra;
    }

    public function testTheBuilderMakesReadableFiles(): void
    {
        // required INT32: 1, 2, 3
        $r = Reader::fromString(FileBuilder::file([self::col(FileBuilder::dataPageV1(self::ints(1, 2, 3), 3))], 3));
        $this->assertSame([1, 2, 3], $r->readColumn(0, 'c'));
        // optional INT32: 7, null, 9 (definition levels 1,0,1)
        $page = FileBuilder::dataPageV1(FileBuilder::defLevelsV1([1, 0, 1]) . self::ints(7, 9), 3);
        $r = Reader::fromString(FileBuilder::file([self::col($page, self::INT32, true)], 3));
        $this->assertSame([7, null, 9], $r->readColumn(0, 'c'));
        // the same as a data page v2 (levels: RLE without the length prefix)
        $levels = \substr(FileBuilder::defLevelsV1([1, 0, 1]), 4);
        $page = FileBuilder::dataPageV2($levels, self::ints(7, 9), 3, 1);
        $r = Reader::fromString(FileBuilder::file([self::col($page, self::INT32, true)], 3));
        $this->assertSame([7, null, 9], $r->readColumn(0, 'c'));
    }

    public function testDataPageV2WithIsCompressedFalseHoldsPlainValuesEvenWhenTheChunkCodecIsSnappy(): void
    {
        // is_compressed=false: the values after the levels are stored as they are, whatever the column's codec says
        $levels = \substr(FileBuilder::defLevelsV1([1, 0, 1]), 4);
        $page = FileBuilder::dataPageV2($levels, self::ints(7, 9), 3, 1); // (dataPageV2 writes is_compressed = false)
        $r = Reader::fromString(FileBuilder::file([self::col($page, self::INT32, true, ['codec' => Codec::Snappy->value])], 3));
        $this->assertSame([7, null, 9], $r->readColumn(0, 'c'));
        $r = Reader::fromString(FileBuilder::file([self::col($page, self::INT32, true, ['codec' => Codec::Zstd->value])], 3));
        $this->assertSame([7, null, 9], $r->readColumn(0, 'c'));
        // and with is_compressed = true the same bytes are taken for a Snappy block and refused
        $bad = FileBuilder::dataPageV2($levels, self::ints(7, 9), 3, 1, 0, [8 => [Thrift::STRUCT, [[1, Thrift::I32, 3], [2, Thrift::I32, 1], [3, Thrift::I32, 3], [4, Thrift::I32, 0],
            [5, Thrift::I32, \strlen($levels)], [6, Thrift::I32, 0], [7, Thrift::BOOL_TRUE, true]]]]);
        $this->expectException(ParquetException::class);
        Reader::fromString(FileBuilder::file([self::col($bad, self::INT32, true, ['codec' => Codec::Snappy->value])], 3))->readColumn(0, 'c');
    }

    public function testZstdBombInAPageIsStoppedByMaxSizeNotAfterwards(): void
    {
        // a frame without content size made of 512 RLE blocks of 128 KiB: 64 MiB of zeros in 2 KB. The page header promises 400 bytes.
        $bomb = "\x28\xB5\x2F\xFD\x00\x40";
        for ($i = 1; $i <= 512; $i++) {
            $h = (131072 << 3) | (1 << 1) | ($i === 512 ? 1 : 0);
            $bomb .= \chr($h & 255) . \chr(($h >> 8) & 255) . \chr($h >> 16) . "\0";
        }
        $page = FileBuilder::dataPageV1($bomb, 100, 0, 400);
        $r = Reader::fromString(FileBuilder::file([self::col($page, self::INT32, false, ['codec' => Codec::Zstd->value])], 100));
        \gc_collect_cycles();
        $base = \memory_get_usage();
        \memory_reset_peak_usage();
        $t = \hrtime(true);
        try {
            $r->readColumn(0, 'c');
            $this->fail('read');
        } catch (ParquetException) {
        }
        $this->assertLessThan(1500, (\hrtime(true) - $t) / 1e6, 'took long: the whole bomb was decompressed?');
        $this->assertLessThan(8 << 20, \memory_get_peak_usage() - $base, 'memory grew by the size of the bomb');
    }

    public function testDeprecatedBitPackedDefinitionLevels(): void
    {
        // data page v1 with definition_level_encoding BIT_PACKED (4): one bit per level, most significant bit first, no length prefix
        $body = "\xA0" . self::ints(7, 9); // levels 1,0,1,0,0,0,0,0
        $page = FileBuilder::dataPageV1($body, 3, 0, null, null, [5 => [Thrift::STRUCT, [[1, Thrift::I32, 3], [2, Thrift::I32, 0], [3, Thrift::I32, 4], [4, Thrift::I32, 4]]]]);
        $r = Reader::fromString(FileBuilder::file([self::col($page, self::INT32, true)], 3));
        $this->assertSame([7, null, 9], $r->readColumn(0, 'c'));
    }

    public function testRleEncodedBooleans(): void
    {
        // RLE booleans in a data page: 4-byte length, then a run of 3 x true and a run of 2 x false (bit width 1)
        $runs = "\x06\x01" . "\x04\x00";
        $page = FileBuilder::dataPageV1(\pack('V', \strlen($runs)) . $runs, 5, 3);
        $r = Reader::fromString(FileBuilder::file([self::col($page, 0)], 5));
        $this->assertSame([true, true, true, false, false], $r->readColumn(0, 'c'));
    }

    public function testDictionaryPageAndIndices(): void
    {
        // dictionary [10, 20, 30]; indices 2,0,1,1 at bit width 2: one bit-packed group (header 0x03), 8 values, 2 bytes
        $dict = FileBuilder::dictionaryPage(self::ints(10, 20, 30), 3);
        $idx = "\x02" . "\x03" . \chr(2 | (0 << 2) | (1 << 4) | (1 << 6)) . "\x00";
        $page = FileBuilder::dataPageV1($idx, 4, 8);
        $r = Reader::fromString(FileBuilder::file([self::col($dict . $page)], 4));
        $this->assertSame([30, 10, 20, 20], $r->readColumn(0, 'c'));
    }

    /** @return array<string, array{callable(): string}> */
    public static function hostile(): array
    {
        $ints = self::ints(1, 2, 3);
        $page3 = FileBuilder::dataPageV1($ints, 3);
        $dict = FileBuilder::dictionaryPage(self::ints(10, 20, 30), 3);
        $cases = [
            'page has more values than the chunk' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1(self::ints(1, 2, 3, 4), 4))], 3),
            'page has fewer values than the chunk' => fn () => FileBuilder::file([self::col($page3)], 5),
            'chunk ends before its values' => fn () => FileBuilder::file([self::col($page3, self::INT32, false, ['numValues' => 3])], 4),
            'page compressed size larger than the chunk' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1($ints, 3, 0, 12, 99999))], 3),
            'negative compressed size' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1($ints, 3, 0, 12, -5))], 3),
            'negative uncompressed size' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1($ints, 3, 0, -12))], 3),
            'uncompressed size does not match (uncompressed page)' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1($ints, 3, 0, 20))], 3),
            'negative number of values in a page' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1($ints, -3))], 3),
            'plain values shorter than the page says' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1(self::ints(1, 2), 3))], 3),
            'dictionary index out of range' => fn () => FileBuilder::file([self::col($dict . FileBuilder::dataPageV1("\x02\x03" . \chr(3 | (3 << 2)) . "\x00", 2, 8))], 2),
            'dictionary-encoded page without a dictionary' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1("\x02\x03\x00\x00", 2, 8))], 2),
            'dictionary page after data' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1(self::ints(1), 1) . $dict . FileBuilder::dataPageV1(self::ints(2, 3), 2))], 3),
            'two dictionary pages' => fn () => FileBuilder::file([self::col($dict . $dict . $page3)], 3),
            'dictionary with a negative size' => fn () => FileBuilder::file([self::col(FileBuilder::dictionaryPage(self::ints(1), -4) . $page3)], 3),
            'dictionary with an absurd value count' => fn () => FileBuilder::file([self::col(FileBuilder::dictionaryPage(self::ints(1), 1 << 40) . FileBuilder::dataPageV1("\x02\x03\x00\x00", 2, 8))], 2),
            'RLE bit width above 32' => fn () => FileBuilder::file([self::col($dict . FileBuilder::dataPageV1("\x21\x03\x00\x00\x00\x00\x00\x00", 2, 8))], 2),
            'definition levels longer than the page' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1(\pack('V', 999) . "\x03\x05" . $ints, 3), self::INT32, true)], 3),
            'definition levels cut off (no length)' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1("\x01", 3), self::INT32, true)], 3),
            'definition level above the maximum' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1(\pack('V', 2) . "\x02\x03" . $ints, 3), self::INT32, true)], 3),
            'a data page v2 whose level lengths exceed the page' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV2('', $ints, 3, 0, 0, [8 => [Thrift::STRUCT, [[1, Thrift::I32, 3], [2, Thrift::I32, 0], [3, Thrift::I32, 3], [4, Thrift::I32, 0], [5, Thrift::I32, 5000], [6, Thrift::I32, 0]]]]))], 3),
            'a data page v2 with a negative level length' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV2('', $ints, 3, 0, 0, [8 => [Thrift::STRUCT, [[1, Thrift::I32, 3], [2, Thrift::I32, 0], [3, Thrift::I32, 3], [4, Thrift::I32, 0], [5, Thrift::I32, -1], [6, Thrift::I32, 0]]]]))], 3),
            'an optional column whose v2 page has no levels' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV2('', $ints, 3, 0), self::INT32, true)], 3),
            'chunk offset beyond the file' => fn () => FileBuilder::file([self::col($page3, self::INT32, false, ['offset' => 1 << 40])], 3),
            'chunk size beyond the file' => fn () => FileBuilder::file([self::col($page3, self::INT32, false, ['totalCompressed' => 1 << 40])], 3),
            'column value count differs from the row count' => fn () => FileBuilder::file([self::col($page3, self::INT32, false, ['numValues' => 2])], 3),
            'footer length larger than the file' => fn () => FileBuilder::file([self::col($page3)], 3, ['footerLength' => 100000]),
            'footer length zero' => fn () => FileBuilder::file([self::col($page3)], 3, ['footerLength' => 0]),
            'footer length 0xFFFFFFFF' => fn () => FileBuilder::file([self::col($page3)], 3, ['footerLength' => 0xFFFFFFFF]),
            'footer length one too short' => fn () => FileBuilder::file([self::col($page3)], 3, ['footerLength' => 30]),
            'a byte array whose length runs past the page' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1(\pack('V', 1 << 30) . 'abc', 1), self::BYTE_ARRAY)], 1),
            'RLE booleans with a length beyond the page' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1(\pack('V', 100) . "\x02\x01", 2, 3), 0)], 2),
            'schema group announcing too many children' => fn () => FileBuilder::file([self::col($page3)], 3, ['schema' => [[[4, Thrift::BINARY, 'schema'], [5, Thrift::I32, 9]], [[1, Thrift::I32, 1], [3, Thrift::I32, 0], [4, Thrift::BINARY, 'c']]]]),
            'schema with leftover elements' => fn () => FileBuilder::file([self::col($page3)], 3, ['schema' => [[[4, Thrift::BINARY, 'schema'], [5, Thrift::I32, 1]], [[1, Thrift::I32, 1], [3, Thrift::I32, 0], [4, Thrift::BINARY, 'c']], [[1, Thrift::I32, 1], [3, Thrift::I32, 0], [4, Thrift::BINARY, 'extra']]]]),
            'schema with an unknown physical type' => fn () => FileBuilder::file([self::col($page3)], 3, ['schema' => [[[4, Thrift::BINARY, 'schema'], [5, Thrift::I32, 1]], [[1, Thrift::I32, 77], [3, Thrift::I32, 0], [4, Thrift::BINARY, 'c']]]]),
            'schema element without a name' => fn () => FileBuilder::file([self::col($page3)], 3, ['schema' => [[[4, Thrift::BINARY, 'schema'], [5, Thrift::I32, 1]], [[1, Thrift::I32, 1], [3, Thrift::I32, 0]]]]),
            'empty schema' => fn () => FileBuilder::file([self::col($page3)], 3, ['schema' => []]),
            'FIXED_LEN_BYTE_ARRAY without a length' => fn () => FileBuilder::file([self::col($page3)], 3, ['schema' => [[[4, Thrift::BINARY, 'schema'], [5, Thrift::I32, 1]], [[1, Thrift::I32, 7], [3, Thrift::I32, 0], [4, Thrift::BINARY, 'c']]]]),
            'a schema nested 100 levels deep' => fn () => FileBuilder::file([self::col($page3)], 3, ['schema' => self::deepSchema(100)]),
            'zstd page that expands far beyond its declared size' => fn () => FileBuilder::file([self::col(FileBuilder::dataPageV1(Zstd::compress(\str_repeat("\0", 4 << 20)), 3, 0, 12), self::INT32, false, ['codec' => Codec::Zstd->value])], 3),
        ];
        $out = [];
        foreach ($cases as $name => $make) {
            $out[$name] = [$make];
        }
        return $out;
    }

    /** @return list<list<array{int, int, mixed}>> */
    private static function deepSchema(int $depth): array
    {
        $el = [[[4, Thrift::BINARY, 'schema'], [5, Thrift::I32, 1]]];
        for ($i = 0; $i < $depth; $i++) {
            $el[] = [[3, Thrift::I32, 0], [4, Thrift::BINARY, "g$i"], [5, Thrift::I32, 1]];
        }
        $el[] = [[1, Thrift::I32, 1], [3, Thrift::I32, 0], [4, Thrift::BINARY, 'leaf']];
        return $el;
    }

    #[DataProvider('hostile')]
    public function testHostileFileIsRefused(callable $make): void
    {
        $bytes = $make();
        $t = \hrtime(true);
        try {
            $r = Reader::fromString($bytes);
            $r->readRowGroup(0);
            $this->fail('the file was read');
        } catch (ParquetException) {
            $this->addToAssertionCount(1);
        }
        $this->assertLessThan(2000, (\hrtime(true) - $t) / 1e6, 'took long to refuse');
    }

    public function testUnknownFieldsInTheFooterAreIgnored(): void
    {
        // a newer writer: extra fields in FileMetaData (a struct with a nested list and a map) must not matter
        $extra = [[99, Thrift::STRUCT, [[1, Thrift::LIST, [Thrift::STRUCT, [[[1, Thrift::BINARY, 'x']]]]], [2, Thrift::MAP, [Thrift::BINARY, Thrift::I64, [['k', 5]]]]]]];
        $r = Reader::fromString(FileBuilder::file([self::col(FileBuilder::dataPageV1(self::ints(1, 2, 3), 3))], 3, ['footerExtra' => $extra]));
        $this->assertSame([1, 2, 3], $r->readColumn(0, 'c'));
    }

    public function testUnknownPageTypeIsUnsupported(): void
    {
        $page = FileBuilder::dataPageV1(self::ints(1), 1, 0, null, null, [1 => [Thrift::I32, 9]]);
        $r = Reader::fromString(FileBuilder::file([self::col($page)], 1));
        $this->expectException(UnsupportedException::class);
        $r->readColumn(0, 'c');
    }

    public function testIndexPagesAreSkipped(): void
    {
        $index = Thrift::struct([[1, Thrift::I32, 1], [2, Thrift::I32, 4], [3, Thrift::I32, 4], [6, Thrift::STRUCT, []]]) . 'abcd';
        $r = Reader::fromString(FileBuilder::file([self::col($index . FileBuilder::dataPageV1(self::ints(5), 1))], 1));
        $this->assertSame([5], $r->readColumn(0, 'c'));
    }

    public function testEmptyDataPagesInTheMiddleAreSkipped(): void
    {
        $chunk = FileBuilder::dataPageV1('', 0) . FileBuilder::dataPageV1(self::ints(1, 2), 2) . FileBuilder::dataPageV1('', 0) . FileBuilder::dataPageV1(self::ints(3), 1);
        $r = Reader::fromString(FileBuilder::file([self::col($chunk)], 3));
        $this->assertSame([1, 2, 3], $r->readColumn(0, 'c'));
    }

    public function testRowCountLargerThanTheLimitIsUnsupportedNotAnAllocation(): void
    {
        $r = Reader::fromString(FileBuilder::file([self::col(FileBuilder::dataPageV1('', 0), self::INT32, false, ['numValues' => 1 << 40])], 1 << 40));
        $this->expectException(UnsupportedException::class);
        $r->readColumn(0, 'c');
    }

    public function testOneExoticColumnDoesNotMakeTheFileUnreadable(): void
    {
        $good = FileBuilder::dataPageV1(self::ints(1, 2, 3), 3);
        $bytes = FileBuilder::file([
            ['name' => 'a', 'type' => self::INT32, 'chunk' => $good],
            ['name' => 'unknown_codec', 'type' => self::INT32, 'chunk' => $good, 'codec' => 99],
            ['name' => 'no_metadata', 'type' => self::INT32, 'chunk' => $good, 'noMetadata' => true],
            ['name' => 'encrypted', 'type' => self::INT32, 'chunk' => $good, 'noMetadata' => true, 'crypto' => true],
            ['name' => 'crypto_with_metadata', 'type' => self::INT32, 'chunk' => $good, 'crypto' => true],
            ['name' => 'b', 'type' => self::INT32, 'chunk' => $good],
        ], 3);
        $r = Reader::fromString($bytes);
        $this->assertSame([1, 2, 3], $r->readColumn(0, 'a'));
        $this->assertSame([1, 2, 3], $r->readColumn(0, 'b'));
        $chunks = $r->metadata()->rowGroups[0]->columns;
        $this->assertNull($chunks[1]->codec);
        $this->assertSame(99, $chunks[1]->codecId);
        foreach (['unknown_codec' => 'codec 99', 'no_metadata' => "no column metadata", 'encrypted' => 'encrypted column metadata', 'crypto_with_metadata' => 'encrypted column crypto_with_metadata'] as $col => $msg) {
            try {
                $r->readColumn(0, $col);
                $this->fail("$col was read");
            } catch (UnsupportedException $e) {
                $this->assertStringContainsString($msg, $e->getMessage());
            }
        }
        // rows() over the readable ones works; asking for all of them names the first that cannot be read
        $this->assertCount(3, \iterator_to_array($r->rows(['a', 'b']), false));
        $this->expectException(UnsupportedException::class);
        \iterator_to_array($r->rows());
    }

    public function testUnknownConvertedTypeAndEncodingIdsParse(): void
    {
        $page = FileBuilder::dataPageV1(self::ints(1, 2, 3), 3);
        $schema = [[[4, Thrift::BINARY, 'schema'], [5, Thrift::I32, 1]], [[1, Thrift::I32, 1], [3, Thrift::I32, 0], [4, Thrift::BINARY, 'c'], [6, Thrift::I32, 77]]];
        $r = Reader::fromString(FileBuilder::file([self::col($page)], 3, ['schema' => $schema]));
        $leaf = $r->schema()->leaf('c');
        $this->assertNull($leaf->convertedType);
        $this->assertSame(77, $leaf->convertedTypeId);
        $this->assertSame([1, 2, 3], $r->readColumn(0, 'c'));
    }

    public function testExceptionsAreParquetExceptions(): void
    {
        $this->assertInstanceOf(ParquetException::class, new ParquetException());
        $this->assertInstanceOf(ParquetException::class, new UnsupportedException());
        $this->assertInstanceOf(\RuntimeException::class, new ParquetException());
    }
}
