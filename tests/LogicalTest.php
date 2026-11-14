<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{FormatException, Reader, ValueMode};
use Charm\Parquet\Internal\Logical;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Logical values: against pyarrow's data read through its INTEGER storage and formatted by the independent Python code in
 * tests/tools/oracle.py (fixtures/expected/*.json "columnsLogical"), against vectors from that Python code, and by hand.
 */
final class LogicalTest extends TestCase
{
    /** @return array<string, array{string}> */
    public static function logicalFixtures(): array
    {
        $out = [];
        foreach (Fixtures::names() as $n) {
            if (isset(Fixtures::expected($n)['columnsLogical'])) {
                $out[$n] = [$n];
            }
        }
        return $out;
    }

    #[DataProvider('logicalFixtures')]
    public function testLogicalValuesEqualTheIndependentExpectation(string $name): void
    {
        $e = Fixtures::expected($name)['columnsLogical'];
        $r = Fixtures::logicalReader($name);
        $this->assertNotSame([], $e);
        foreach ($e as $path => $col) {
            $got = [];
            for ($g = 0; $g < $r->rowGroupCount(); $g++) {
                foreach ($r->readColumn($g, $path) as $v) {
                    $got[] = Canonical::logical($v);
                }
            }
            $this->assertCount(\count($col['values']), $got, "$name $path");
            foreach ($col['values'] as $i => $want) {
                if ($got[$i] !== $want) {
                    $this->fail("$name column $path row $i: got " . \var_export($got[$i], true) . ' (' . (\is_string($got[$i]) ? \hex2bin($got[$i]) : '') . '), expected ' . \var_export($want, true) . ' (' . (\is_string($want) ? @\hex2bin($want) : '') . ')');
                }
            }
            $this->addToAssertionCount(1);
        }
    }

    public function testTypesOfTheValues(): void
    {
        $r = Fixtures::logicalReader('logical-edges');
        $u64 = $r->readColumn(0, 'u64');
        $this->assertSame(0, $u64[0]);
        $this->assertSame('18446744073709551615', $u64[1]);
        $this->assertSame('9223372036854775808', $u64[2]);
        $this->assertSame(\PHP_INT_MAX, $u64[3]); // fits: an int
        $this->assertSame('10000000000000000000', $u64[4]);
        $this->assertSame('18446744073709551610', $u64[5]);
        $this->assertSame([0, 4294967295, 2147483648, 1], \array_slice($r->readColumn(0, 'u32'), 0, 4));
        $this->assertSame([0, 255, 128, 1], \array_slice($r->readColumn(0, 'u8'), 0, 4));
        $this->assertSame([-128, 127, 0, -1], \array_slice($r->readColumn(0, 'i8'), 0, 4));
        $this->assertSame('1970-01-01', $r->readColumn(0, 'date')[0]);
        $us = $r->readColumn(0, 'ts_us_utc');
        $this->assertSame('1970-01-01T00:00:00.000001Z', $us[1]);
        $this->assertSame('1969-12-31T23:59:59.999999Z', $us[2]);
        $this->assertSame('2000-02-29T00:00:00.000000Z', $us[5]);
        $this->assertSame('0001-01-01T00:00:00.000000Z', $us[7]);
        $this->assertSame('0000-12-31T23:59:59.999999Z', $us[8]);
        $this->assertSame('9999-12-31T23:59:59.999999Z', $us[9]);
        $this->assertSame('10000-01-01T00:00:00.000000Z', $us[10]);
        $this->assertSame('1970-01-01T00:00:00.000Z', $r->readColumn(0, 'ts_ms_utc')[0]);
        $this->assertSame('1970-01-01T00:00:00.000000000', $r->readColumn(0, 'ts_ns')[0]);
        $this->assertSame('00000000-0000-0000-0000-000000000000', $r->readColumn(0, 'uuid')[2]);
        $this->assertSame('123e4567-e89b-12d3-a456-426614174000', $r->readColumn(0, 'uuid')[3]);
        $this->assertSame('abc', $r->readColumn(0, 'str')[1]);
        $f16 = $r->readColumn(0, 'f16');
        $this->assertSame(65504.0, $f16[3]);
        $this->assertSame(6.103515625e-05, $f16[4]);
        $this->assertTrue(\is_nan($f16[8]));
        $this->assertSame(\INF, $f16[6]);
        $this->assertSame(-\INF, \fdiv(1, $f16[1])); // negative zero
    }

    public function testPhysicalModeKeepsToday(): void
    {
        $p = Fixtures::reader('logical-edges');
        $this->assertSame(0, $p->readColumn(0, 'date')[0]);
        $this->assertSame(-1, $p->readColumn(0, 'u64')[1]);
        $this->assertSame(255, $p->readColumn(0, 'u8')[1]);
        $this->assertSame(4294967295 - 4294967296, $p->readColumn(0, 'u32')[1]);
        $this->assertSame(16, \strlen($p->readColumn(0, 'uuid')[0]));
        $this->assertSame(2, \strlen($p->readColumn(0, 'f16')[0]));
        $this->assertSame(12, \strlen(Fixtures::reader('int96')->readColumn(0, 'ts')[0]));
    }

    public function testLogicalIsTheDefault(): void
    {
        $r = Reader::fromFile(Fixtures::path('logical-edges'));
        $this->assertSame('1970-01-01', $r->readColumn(0, 'date')[0]);
        $r = Reader::fromString(Fixtures::bytes('logical-edges'));
        $this->assertSame('1970-01-01', $r->readColumn(0, 'date')[0]);
    }

    public function testDecimalVectorsFromPython(): void
    {
        $v = \json_decode((string) \file_get_contents(__DIR__ . '/fixtures/vectors.json'), true, 512, \JSON_THROW_ON_ERROR)['decimal'];
        $this->assertGreaterThan(500, \count($v));
        $lengths = [];
        foreach ($v as [$hex, $scale, $want]) {
            $bytes = \hex2bin($hex);
            $lengths[\strlen($bytes)] = true;
            $this->assertSame($want, Logical::decimalBytes($bytes, $scale), "$hex scale $scale");
        }
        \ksort($lengths);
        $this->assertSame(\range(0, 32), \array_keys($lengths), 'every length from 0 to 32 bytes');
    }

    public function testDecimalEdgeCases(): void
    {
        $this->assertSame('0', Logical::decimalBytes("\x00", 0));
        $this->assertSame('-1', Logical::decimalBytes("\xFF", 0));
        $this->assertSame('-128', Logical::decimalBytes("\x80", 0));
        $this->assertSame('127', Logical::decimalBytes("\x7F", 0));
        $this->assertSame('0.05', Logical::decimalBytes("\x05", 2));
        $this->assertSame('-0.05', Logical::decimalBytes("\xFB", 2));
        $this->assertSame('0.000', Logical::decimalBytes('', 3));
        $this->assertSame('-9223372036854775808', Logical::decimalBytes("\x80\x00\x00\x00\x00\x00\x00\x00", 0));
        $this->assertSame('9223372036854775807', Logical::decimalBytes("\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 0));
        $this->assertSame('18446744073709551616', Logical::decimalBytes("\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00", 0));
        $this->assertSame('-9223372036854775809', Logical::decimalBytes("\xFF\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 0));
        $this->assertSame('170141183460469231731687303715884105727', Logical::decimalBytes("\x7F" . \str_repeat("\xFF", 15), 0));
        $this->assertSame('-170141183460469231731687303715884105728', Logical::decimalBytes("\x80" . \str_repeat("\x00", 15), 0));
        $this->assertSame('-1.00', Logical::decimalInt(-100, 2));
        $this->assertSame('-9223372036854775808', Logical::decimalInt(\PHP_INT_MIN, 0));
        $this->assertSame('-92233720368547758.08', Logical::decimalInt(\PHP_INT_MIN, 2));
        $this->assertSame('0.0000000001', Logical::decimalInt(1, 10));
        $this->assertSame('123', Logical::decimalInt(123, 0));
    }

    public function testDecimalOfAbsurdLengthIsRefusedNotComputed(): void
    {
        $this->expectException(\Charm\Parquet\UnsupportedException::class);
        Logical::decimalBytes(\str_repeat("\x7F", 500), 0);
    }

    public function testDateVectorsFromPython(): void
    {
        $v = \json_decode((string) \file_get_contents(__DIR__ . '/fixtures/vectors.json'), true, 512, \JSON_THROW_ON_ERROR)['date'];
        foreach ($v as [$days, $want]) {
            $this->assertSame($want, Logical::dateString($days), "day $days");
        }
        $this->assertSame('-5877641-06-23', Logical::dateString(-2147483648)); // a year far below 0000: signed
        $this->assertSame('0000-01-01', Logical::dateString(-719528));
        $this->assertSame('-0001-12-31', Logical::dateString(-719529));
        $this->assertSame('10000-01-01', Logical::dateString(2932897));
    }

    public function testTimestampAndTimeVectorsFromPython(): void
    {
        $v = \json_decode((string) \file_get_contents(__DIR__ . '/fixtures/vectors.json'), true, 512, \JSON_THROW_ON_ERROR);
        $units = ['ms' => [1000, 3, \Charm\Parquet\TimeUnit::Millis], 'us' => [1000000, 6, \Charm\Parquet\TimeUnit::Micros], 'ns' => [1000000000, 9, \Charm\Parquet\TimeUnit::Nanos]];
        foreach ($v['time'] as [$unit, $x, $want]) {
            [$per, $digits] = $units[$unit];
            $this->assertSame($want, Logical::timeString($x, $per, $digits), "$unit $x");
        }
        // timestamps through a file that is built by hand, once per unit and per UTC flag
        foreach ($units as $unit => [$per, $digits, $tu]) {
            foreach ([false, true] as $utc) {
                $rows = \array_values(\array_filter($v['timestamp'], static fn ($t) => $t[0] === $unit));
                $leaf = [[1, Thrift::I32, 2], [3, Thrift::I32, 0], [4, Thrift::BINARY, 'c'], self::timestampType($utc, $tu->value)];
                $plain = '';
                foreach ($rows as $row) {
                    $plain .= \pack('P', $row[1]);
                }
                $page = FileBuilder::dataPageV1($plain, \count($rows));
                $bytes = FileBuilder::file([['name' => 'c', 'type' => 2, 'chunk' => $page]], \count($rows), ['schema' => [[[4, Thrift::BINARY, 'schema'], [5, Thrift::I32, 1]], $leaf]]);
                $got = Reader::fromString($bytes, ValueMode::Logical)->readColumn(0, 'c');
                foreach ($rows as $i => $row) {
                    $this->assertSame($utc ? $row[3] : $row[2], $got[$i], "$unit {$row[1]} utc=" . (int) $utc);
                }
            }
        }
    }

    /** The SchemaElement field 10 (logicalType) holding TIMESTAMP(isAdjustedToUTC, unit 1 = MILLIS, 2 = MICROS, 3 = NANOS). */
    private static function timestampType(bool $utc, int $unit): array
    {
        $unitStruct = [[$unit, Thrift::STRUCT, []]];
        $timestamp = [[1, Thrift::BOOL_TRUE, $utc], [2, Thrift::STRUCT, $unitStruct]];
        return [10, Thrift::STRUCT, [[8, Thrift::STRUCT, $timestamp]]];
    }

    /** Builds a one-column file whose schema element has the given extra fields (converted type, logical type, ...) and reads it. */
    private function column(int $physical, array $schemaFields, string $values, int $n, ?int $typeLength = null, ValueMode $mode = ValueMode::Logical): array
    {
        $leaf = [[1, Thrift::I32, $physical], ...($typeLength !== null ? [[2, Thrift::I32, $typeLength]] : []), [3, Thrift::I32, 0], [4, Thrift::BINARY, 'c'], ...$schemaFields];
        \usort($leaf, static fn ($a, $b) => $a[0] <=> $b[0]);
        $col = ['name' => 'c', 'type' => $physical, 'chunk' => FileBuilder::dataPageV1($values, $n)] + ($typeLength !== null ? ['typeLength' => $typeLength] : []);
        $bytes = FileBuilder::file([$col], $n, ['schema' => [[[4, Thrift::BINARY, 'schema'], [5, Thrift::I32, 1]], $leaf]]);
        return Reader::fromString($bytes, $mode)->readColumn(0, 'c');
    }

    public function testLegacyConvertedTypesWithoutALogicalType(): void
    {
        // DATE (6) on INT32
        $this->assertSame(['1970-01-02', '1969-12-31'], $this->column(1, [[6, Thrift::I32, 6]], \pack('V*', 1, 0xFFFFFFFF), 2));
        // UINT_32 (13), UINT_8 (11)
        $this->assertSame([4294967295, 1], $this->column(1, [[6, Thrift::I32, 13]], \pack('V*', 0xFFFFFFFF, 1), 2));
        $this->assertSame([255, 1], $this->column(1, [[6, Thrift::I32, 11]], \pack('V*', 0xFFFFFFFF, 1), 2));
        // UINT_64 (14)
        $this->assertSame(['18446744073709551615', 5], $this->column(2, [[6, Thrift::I32, 14]], \pack('P*', -1, 5), 2));
        // INT_16 (16): unchanged
        $this->assertSame([-5], $this->column(1, [[6, Thrift::I32, 16]], \pack('V', 0xFFFFFFFB), 1));
        // DECIMAL (5) on INT32 and on BYTE_ARRAY, scale and precision in the SchemaElement itself
        $this->assertSame(['-1.50', '0.07'], $this->column(1, [[6, Thrift::I32, 5], [7, Thrift::I32, 2], [8, Thrift::I32, 5]], \pack('V*', 0xFFFFFF6A, 7), 2));
        // (the 13 bytes 09fd06a0cd0e2a3b2e0d4f1a8c are 791361183029337516092536330892 as Python's int.from_bytes reads them)
        $this->assertSame(['7913611830293375160925363308.92'], $this->column(6, [[6, Thrift::I32, 5], [7, Thrift::I32, 2], [8, Thrift::I32, 34]], \pack('V', 13) . \hex2bin('09fd06a0cd0e2a3b2e0d4f1a8c'), 1));
        // TIME_MILLIS (7) on INT32, TIME_MICROS (8) on INT64
        $this->assertSame(['01:00:01.250'], $this->column(1, [[6, Thrift::I32, 7]], \pack('V', 3601250), 1));
        $this->assertSame(['00:00:00.000005'], $this->column(2, [[6, Thrift::I32, 8]], \pack('P', 5), 1));
        // TIMESTAMP_MILLIS (9) and TIMESTAMP_MICROS (10): legacy timestamps are UTC
        $this->assertSame(['1970-01-01T00:00:01.500Z'], $this->column(2, [[6, Thrift::I32, 9]], \pack('P', 1500), 1));
        $this->assertSame(['1969-12-31T23:59:59.999999Z'], $this->column(2, [[6, Thrift::I32, 10]], \pack('P', -1), 1));
        // INTERVAL (21) on FIXED_LEN_BYTE_ARRAY(12)
        $this->assertSame([['months' => 14, 'days' => 3, 'milliseconds' => 4000000000]], $this->column(7, [[6, Thrift::I32, 21]], \pack('V3', 14, 3, 4000000000), 1, 12));
        // UTF8 (0), ENUM (4), JSON (19), BSON (20): the bytes
        foreach ([0, 4, 19, 20] as $ct) {
            $this->assertSame(["a\x00\xff"], $this->column(6, [[6, Thrift::I32, $ct]], \pack('V', 3) . "a\x00\xff", 1));
        }
    }

    public function testLogicalTypeWinsOverConvertedType(): void
    {
        // converted type DATE, but a logical TIMESTAMP(ms, not adjusted) on INT64: the logical type rules
        $this->assertSame(['1970-01-01T00:00:01.500'], $this->column(2, [[6, Thrift::I32, 6], self::timestampType(false, 1)], \pack('P', 1500), 1));
    }

    public function testAnnotationsThatDoNotFitThePhysicalTypeAreIgnored(): void
    {
        // DATE on INT64, UUID on INT32, TIMESTAMP on INT32: physical values
        $this->assertSame([5], $this->column(2, [[6, Thrift::I32, 6]], \pack('P', 5), 1));
        $this->assertSame([5], $this->column(1, [[10, Thrift::STRUCT, [[14, Thrift::STRUCT, []]]]], \pack('V', 5), 1));
        $this->assertSame([5], $this->column(1, [self::timestampType(true, 1)], \pack('V', 5), 1));
        // an annotation this version has never heard of (logical field 99), and the UNKNOWN logical type: physical values
        $this->assertSame([5], $this->column(1, [[10, Thrift::STRUCT, [[99, Thrift::STRUCT, []]]]], \pack('V', 5), 1));
        $this->assertSame([5], $this->column(1, [[10, Thrift::STRUCT, [[11, Thrift::STRUCT, []]]]], \pack('V', 5), 1));
    }

    public function testUuidOfTheWrongLengthIsLeftAlone(): void
    {
        $this->assertSame(["\x01\x02\x03\x04"], $this->column(7, [[10, Thrift::STRUCT, [[14, Thrift::STRUCT, []]]]], "\x01\x02\x03\x04", 1, 4));
    }

    public function testInt96IsATimestampWithNineDigits(): void
    {
        $raw = \pack('P', 1) . \pack('l', 2440588) . \pack('P', 86399999999999) . \pack('l', 2440588 + 1) . \pack('P', 0) . \pack('l', 2440587);
        $this->assertSame(['1970-01-01T00:00:00.000000001', '1970-01-02T23:59:59.999999999', '1969-12-31T00:00:00.000000000'], $this->column(3, [], $raw, 3));
    }

    public function testBadDecimalScaleIsAFormatException(): void
    {
        $this->expectException(FormatException::class);
        $this->column(1, [[6, Thrift::I32, 5], [7, Thrift::I32, -3], [8, Thrift::I32, 5]], \pack('V', 1), 1);
    }

    public function testNullsStayNull(): void
    {
        $r = Fixtures::logicalReader('decimal-int');
        $this->assertNull($r->readColumn(0, 'd9')[1]);
        $this->assertSame('1234567.89', $r->readColumn(0, 'd9')[0]);
        $r = Fixtures::logicalReader('logical-edges');
        $u8 = $r->readColumn(0, 'u8');
        $this->assertNull($u8[4]);
        $this->assertSame(1, $u8[3]);
    }
}
