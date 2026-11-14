<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{ParquetException, Reader};
use PHPUnit\Framework\TestCase;

final class RangeTest extends TestCase
{
    public function testFromFileReadsTheFooterWithOneRangeCall(): void
    {
        $r = Fixtures::reader('matrix-none-v1-dict');
        $this->assertSame(1, $r->readCalls());
        $this->assertSame(250, $r->numRows());
        $this->assertSame(1, $r->readCalls(), 'metadata and schema need no more reads');
        $r->schema();
        $r->metadata();
        $this->assertSame(1, $r->readCalls());
    }

    public function testEachColumnChunkIsOneRead(): void
    {
        $r = Fixtures::reader('rowgroups'); // 3 row groups
        $r->readColumn(0, 'r_int32');
        $this->assertSame(2, $r->readCalls());
        $r->readRowGroup(1, ['r_int32', 'o_string', 'r_int64']);
        $this->assertSame(5, $r->readCalls());
        // rows() reads each selected column of each row group once
        $before = $r->readCalls();
        \iterator_to_array($r->rows(['r_int32', 'o_string']), false);
        $this->assertSame($before + 3 * 2, $r->readCalls());
    }

    public function testZeroRowGroupsNeedNoColumnReads(): void
    {
        $r = Fixtures::reader('empty-zero-row-group');
        $before = $r->readCalls();
        $r->readColumn(0, 'a');
        $r->readColumn(2, 'b');
        $this->assertSame($before, $r->readCalls());
    }

    public function testFooterLargerThanTheSpeculativeTailTakesASecondRead(): void
    {
        $r = Fixtures::reader('big-footer');
        $this->assertSame(2, $r->readCalls());
        $this->assertSame(33000, \strlen($r->metadata()->keyValueMetadata['big']));
        $this->assertSame(20, $r->numRows());
    }

    public function testAnyRangeFunctionWorksAndNeverReadsOutsideTheFile(): void
    {
        $bytes = Fixtures::bytes('matrix-snappy-v2-dict');
        $calls = [];
        $r = new Reader(static function (int $offset, int $length) use ($bytes, &$calls): string {
            $calls[] = [$offset, $length];
            return \substr($bytes, $offset, $length);
        }, \strlen($bytes));
        $all = $r->readRowGroup(0);
        $ref = Reader::fromString($bytes)->readRowGroup(0);
        $this->assertSame(\array_keys($ref), \array_keys($all));
        foreach ($ref as $path => $vals) { // (compared through the canonical form: the columns hold NaN)
            $t = $r->schema()->leaf($path)->physicalType;
            $this->assertSame(Canonical::column($t, $vals), Canonical::column($t, $all[$path]), $path);
        }
        foreach ($calls as [$o, $l]) {
            $this->assertGreaterThanOrEqual(0, $o);
            $this->assertGreaterThanOrEqual(0, $l);
            $this->assertLessThanOrEqual(\strlen($bytes), $o + $l);
        }
        $this->assertCount(1 + 16, $calls);
    }

    public function testFromStringAndFromFileAgree(): void
    {
        $a = Reader::fromString(Fixtures::bytes('null-patterns'));
        $b = Fixtures::reader('null-patterns');
        $this->assertSame(\iterator_to_array($a->rows(), false), \iterator_to_array($b->rows(), false));
    }

    public function testShortReadIsAnError(): void
    {
        $bytes = Fixtures::bytes('matrix-none-v1-dict');
        $this->expectException(\UnexpectedValueException::class);
        new Reader(static fn (int $o, int $l): string => \substr($bytes, $o, \max(0, $l - 1)), \strlen($bytes));
    }

    public function testRangeFunctionExceptionsPassThroughUnchanged(): void
    {
        $bytes = Fixtures::bytes('matrix-none-v1-dict');
        $thrown = new \DomainException('network down');
        try {
            new Reader(static fn (int $o, int $l): string => throw $thrown, \strlen($bytes));
            $this->fail('expected the range function\'s exception');
        } catch (\DomainException $e) {
            $this->assertSame($thrown, $e);
        }
    }

    public function testRangeFunctionReturningANonStringIsAnUnexpectedValue(): void
    {
        $bytes = Fixtures::bytes('matrix-none-v1-dict');
        $this->expectException(\UnexpectedValueException::class);
        new Reader(static fn (int $o, int $l) => null, \strlen($bytes)); // @phpstan-ignore-line
    }

    public function testMissingFile(): void
    {
        $this->expectException(\RuntimeException::class);
        Reader::fromFile(__DIR__ . '/fixtures/does-not-exist.parquet');
    }

    public function testMissingFileIsNotADataError(): void
    {
        try {
            Reader::fromFile(__DIR__ . '/fixtures/does-not-exist.parquet');
            $this->fail('expected a RuntimeException');
        } catch (\RuntimeException $e) {
            $this->assertNotInstanceOf(\UnexpectedValueException::class, $e);
        }
    }

    public function testUnreadableFile(): void
    {
        $this->expectException(\RuntimeException::class);
        Reader::fromFile(__DIR__ . '/fixtures'); // a directory: it has a size but cannot be read as a file
    }

    public function testExceptionHierarchy(): void
    {
        $this->assertInstanceOf(\UnexpectedValueException::class, new ParquetException());
        $this->assertInstanceOf(ParquetException::class, new \Charm\Parquet\UnsupportedException());
        $this->assertFalse(\class_exists(\Charm\Parquet\FormatException::class));
    }

    public function testNotParquet(): void
    {
        foreach (['', 'PAR1', 'PAR1PAR1', \str_repeat("\0", 100), 'PAR1' . \str_repeat('x', 100) . "\x05\x00\x00\x00PAR1"] as $bytes) {
            try {
                Reader::fromString($bytes);
                $this->fail('accepted: ' . \bin2hex(\substr($bytes, 0, 8)));
            } catch (ParquetException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testEncryptedFooterIsUnsupported(): void
    {
        $bytes = 'PAR1' . \str_repeat('x', 20) . \pack('V', 20) . 'PARE';
        $this->expectException(\Charm\Parquet\UnsupportedException::class);
        $this->expectExceptionMessage('encrypted footer');
        Reader::fromString($bytes);
    }
}
