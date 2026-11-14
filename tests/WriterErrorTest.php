<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Codec, Reader, TimeUnit, Type, UnsupportedException, Writer};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Every way encode() refuses a row, the message it gives, and that the writer is unharmed afterwards. */
final class WriterErrorTest extends TestCase
{
    /** @return array<string, array{Type, mixed, class-string<\Throwable>}> */
    public static function badValues(): array
    {
        $us = TimeUnit::Micros;
        return [
            'boolean given int' => [Type::boolean(), 1, \TypeError::class],
            'int given string' => [Type::int64(), '5', \TypeError::class],
            'int given float' => [Type::int32(), 1.5, \TypeError::class],
            'int given bool' => [Type::int32(), true, \TypeError::class],
            'int8 too big' => [Type::int8(), 128, \ValueError::class],
            'int8 too small' => [Type::int8(), -129, \ValueError::class],
            'int16 too big' => [Type::int16(), 32768, \ValueError::class],
            'int32 too big' => [Type::int32(), 2 ** 31, \ValueError::class],
            'uint8 negative' => [Type::uint8(), -1, \ValueError::class],
            'uint16 too big' => [Type::uint16(), 65536, \ValueError::class],
            'uint32 too big' => [Type::uint32(), 2 ** 32, \ValueError::class],
            'uint64 negative' => [Type::uint64(), -1, \ValueError::class],
            'uint64 too big string' => [Type::uint64(), '18446744073709551616', \ValueError::class],
            'float given string' => [Type::float(), 'x', \TypeError::class],
            'double given array' => [Type::double(), [], \TypeError::class],
            'float16 too big' => [Type::float16(), 1e10, \ValueError::class],
            'string given int' => [Type::string(), 5, \TypeError::class],
            'string not UTF-8' => [Type::string(), "\xff\xfe", \ValueError::class],
            'json not UTF-8' => [Type::json(), "\xff", \ValueError::class],
            'bytes given array' => [Type::bytes(), [1], \TypeError::class],
            'fixed wrong length' => [Type::fixed(4), 'abc', \ValueError::class],
            'decimal not a number' => [Type::decimal(10, 2), 'abc', \ValueError::class],
            'decimal too many fraction digits' => [Type::decimal(10, 2), '1.234', \ValueError::class],
            'decimal too many digits' => [Type::decimal(5, 2), '12345.00', \ValueError::class],
            'decimal given float' => [Type::decimal(10, 2), 1.5, \TypeError::class],
            'date not a date' => [Type::date(), '2020-13-01', \ValueError::class],
            'date not a calendar day' => [Type::date(), '2021-02-29', \ValueError::class],
            'date given float' => [Type::date(), 1.5, \TypeError::class],
            'date out of 32 bits' => [Type::date(), 2 ** 40, \ValueError::class],
            'time bad text' => [Type::time($us), '10:61:00', \ValueError::class],
            'time given float' => [Type::time($us), 1.5, \TypeError::class],
            'time given bool' => [Type::time($us), true, \TypeError::class],
            'time millis too many digits' => [Type::time(TimeUnit::Millis), '10:00:00.1234', \ValueError::class],
            'timestamp bad text' => [Type::timestamp($us, true), 'yesterday', \ValueError::class],
            'timestamp Z on a local type' => [Type::timestamp($us), '2020-01-01T00:00:00Z', \ValueError::class],
            'timestamp given float' => [Type::timestamp($us), 0.5, \TypeError::class],
            'timestamp does not fit' => [Type::timestamp(TimeUnit::Nanos, true), '9999-01-01T00:00:00', \ValueError::class],
            'uuid malformed' => [Type::uuid(), '1234', \ValueError::class],
            'uuid given int' => [Type::uuid(), 5, \TypeError::class],
            'list given string' => [Type::list(Type::int32()), 'x', \TypeError::class],
            'list element wrong' => [Type::list(Type::int32()), [1, 'two'], \TypeError::class],
            'list element out of range' => [Type::list(Type::int8()), [1, 300], \ValueError::class],
            'list holds null in required element' => [Type::list(Type::int32()->required()), [1, null], \ValueError::class],
            'struct given int' => [Type::struct(['a' => Type::int32()]), 5, \TypeError::class],
            'struct unknown field' => [Type::struct(['a' => Type::int32()]), ['b' => 1], \ValueError::class],
            'struct required field null' => [Type::struct(['a' => Type::int32()->required()]), ['a' => null], \ValueError::class],
            'struct field wrong' => [Type::struct(['a' => Type::int32()]), ['a' => 'x'], \TypeError::class],
            'map given int' => [Type::map(Type::string(), Type::int32()), 5, \TypeError::class],
            'map value wrong' => [Type::map(Type::string(), Type::int32()), ['a' => 'x'], \TypeError::class],
            'map pair of three' => [Type::map(Type::string(), Type::int32()), [['a', 1, 2]], \TypeError::class],
            'map null key' => [Type::map(Type::string(), Type::int32()), [[null, 1]], \ValueError::class],
        ];
    }

    #[DataProvider('badValues')]
    public function testBadValueIsRefusedWithColumnAndRow(Type $type, mixed $value, string $class): void
    {
        $w = new Writer(['id' => Type::int64()->required(), 'v' => $type]);
        $w->encode(['id' => 1, 'v' => null]);
        try {
            $w->encode(['id' => 2, 'v' => $value]);
            $this->fail('the row was accepted');
        } catch (\Throwable $e) {
            $this->assertInstanceOf($class, $e, $e::class . ': ' . $e->getMessage());
            $this->assertStringContainsString("column 'v'", $e->getMessage());
            $this->assertStringContainsString('row 1', $e->getMessage());
        }
    }

    #[DataProvider('badValues')]
    public function testWriterIsUsableAndUnchangedAfterARefusedRow(Type $type, mixed $value, string $class): void
    {
        $schema = ['id' => Type::int64()->required(), 'v' => $type, 'w' => Type::list(Type::string())];
        $good = [['id' => 1, 'v' => null, 'w' => ['a', 'b']], ['id' => 3, 'v' => null, 'w' => []]];
        $dirty = new Writer($schema, dataPageSize: 16);
        $out = $dirty->encode($good[0]);
        foreach ([1, 2] as $_) {
            try {
                $dirty->encode(['id' => 2, 'v' => $value, 'w' => ['x', 'y']]);
                $this->fail('the row was accepted');
            } catch (\TypeError|\ValueError) {
            }
        }
        $this->assertSame(1, $dirty->rowCount());
        $out .= $dirty->encode($good[1]) . $dirty->finish();
        $this->assertSame(WriterHelper::write($schema, $good, ['dataPageSize' => 16]), $out);
    }

    public function testTimesBeyondADayAreAcceptedBecauseTheReaderReturnsThem(): void
    {
        $w = new Writer(['t' => Type::time(TimeUnit::Micros)]);
        $out = $w->encode(['t' => '25:00:00']) . $w->encode(['t' => '-00:00:01.5']) . $w->finish();
        $this->assertSame([['t' => '25:00:00.000000'], ['t' => '-00:00:01.500000']], WriterHelper::read($out));
    }

    public function testRowsAfterARefusedRowAreStillBitIdenticalToACleanRun(): void
    {
        $schema = ['id' => Type::int64()->required(), 'n' => Type::list(Type::struct(['a' => Type::int32(), 'b' => Type::string()]))];
        $rows = [['id' => 1, 'n' => [['a' => 1, 'b' => 'x']]], ['id' => 2, 'n' => null], ['id' => 3, 'n' => [['a' => null, 'b' => 'z'], ['a' => 4, 'b' => null]]]];
        $clean = WriterHelper::write($schema, $rows, ['dataPageSize' => 8]);
        $w = new Writer($schema, dataPageSize: 8);
        $out = '';
        foreach ($rows as $i => $row) {
            // a row whose first element is fine and whose second is bad: nothing of it may stay behind
            try {
                $w->encode(['id' => 99, 'n' => [['a' => 1, 'b' => 'ok'], ['a' => 'bad', 'b' => 'x']]]);
                $this->fail('accepted');
            } catch (\TypeError) {
            }
            $out .= $w->encode($row);
        }
        $out .= $w->finish();
        // created_by and everything else is the same, so the bytes are the same
        $this->assertSame($clean, $out);
    }

    public function testUnknownKeyAndNullInRequiredColumn(): void
    {
        $w = new Writer(['id' => Type::int64()->required(), 'name' => Type::string()]);
        try {
            $w->encode(['id' => 1, 'nmae' => 'x']);
            $this->fail('accepted');
        } catch (\ValueError $e) {
            $this->assertStringContainsString("unknown column 'nmae'", $e->getMessage());
            $this->assertStringContainsString('row 0', $e->getMessage());
        }
        foreach ([['name' => 'x'], ['id' => null]] as $row) {
            try {
                $w->encode($row);
                $this->fail('accepted');
            } catch (\ValueError $e) {
                $this->assertStringContainsString("column 'id'", $e->getMessage());
            }
        }
        $out = $w->encode(['id' => 1]);  // a missing optional key is null
        $rows = WriterHelper::read($out . $w->finish());
        $this->assertSame([['id' => 1, 'name' => null]], $rows);
    }

    public function testNestedColumnPathInTheMessage(): void
    {
        $w = new Writer(['a' => Type::struct(['b' => Type::list(Type::int8())])]);
        try {
            $w->encode(['a' => ['b' => [1, 2, 999]]]);
            $this->fail('accepted');
        } catch (\ValueError $e) {
            $this->assertStringContainsString("column 'a'", $e->getMessage());
            $this->assertStringContainsString('999', $e->getMessage());
        }
    }

    public function testFinishedWriterThrowsError(): void
    {
        $w = new Writer(['a' => Type::int32()]);
        $w->finish();
        $this->expectException(\Error::class);
        $w->encode(['a' => 1]);
    }

    public function testFinishTwiceThrowsError(): void
    {
        $w = new Writer(['a' => Type::int32()]);
        $w->finish();
        $this->expectException(\Error::class);
        $w->finish();
    }

    public function testEmptyWriterProducesAValidFile(): void
    {
        foreach ([Codec::Uncompressed, Codec::Snappy, Codec::Gzip, Codec::Zstd] as $codec) {
            $w = new Writer(['a' => Type::int32(), 's' => Type::list(Type::string())], codec: $codec);
            $bytes = $w->finish();
            $this->assertStringStartsWith('PAR1', $bytes);
            $this->assertStringEndsWith('PAR1', $bytes);
            $r = Reader::fromString($bytes);
            $this->assertSame(0, $r->numRows());
            $this->assertSame(0, $r->rowGroupCount());
            $this->assertSame([], \iterator_to_array($r->rows()));
            $this->assertSame(['a', 's'], \array_map(static fn ($c) => $c->name, $r->schema()->root->children));
        }
    }

    public function testConstructorRefusals(): void
    {
        foreach ([
            [fn () => new Writer([]), \ValueError::class],
            [fn () => new Writer(['a' => 'int']), \TypeError::class],
            [fn () => new Writer(['' => Type::int32()]), \ValueError::class],
            [fn () => new Writer(['a' => Type::int32()], codec: Codec::Brotli), \ValueError::class],
            [fn () => new Writer(['a' => Type::int32()], codec: Codec::Lz4Raw), \ValueError::class],
            [fn () => new Writer(['a' => Type::int32()], pageVersion: 3), \ValueError::class],
            [fn () => new Writer(['a' => Type::int32()], rowGroupRows: 0), \ValueError::class],
            [fn () => Type::decimal(39, 0), \ValueError::class],
            [fn () => Type::decimal(5, 6), \ValueError::class],
            [fn () => Type::fixed(0), \ValueError::class],
            [fn () => Type::struct([]), \ValueError::class],
        ] as $i => [$f, $class]) {
            try {
                $f();
                $this->fail("case $i was accepted");
            } catch (\Throwable $e) {
                $this->assertInstanceOf($class, $e, "case $i: " . $e::class . ' ' . $e->getMessage());
            }
        }
    }

    public function testAcceptedAlternativeForms(): void
    {
        $schema = [
            'dec' => Type::decimal(18, 2), 'dt' => Type::date(), 'ts' => Type::timestamp(TimeUnit::Micros, true), 'm' => Type::map(Type::string(), Type::int32()),
            'u' => Type::uint64(), 'h' => Type::float16(), 'tm' => Type::time(TimeUnit::Millis),
        ];
        $w = new Writer($schema);
        $out = $w->encode([
            'dec' => 5, 'dt' => new \DateTimeImmutable('2020-02-29'), 'ts' => new \DateTime('2020-01-01 00:00:00.5', new \DateTimeZone('UTC')),
            'm' => [['a', 1], ['b', 2]], 'u' => '18446744073709551615', 'h' => 0.5, 'tm' => 3600000,
        ]);
        $out .= $w->encode(['dec' => '-1.5', 'dt' => 18321, 'ts' => 1000000, 'm' => ['a' => 1], 'u' => 7, 'h' => 1.0, 'tm' => '01:02:03.004']);
        $rows = WriterHelper::read($out . $w->finish());
        $this->assertSame('5.00', $rows[0]['dec']);
        $this->assertSame('2020-02-29', $rows[0]['dt']);
        $this->assertSame('2020-01-01T00:00:00.500000Z', $rows[0]['ts']);
        $this->assertSame('-1.50', $rows[1]['dec']);
        $this->assertSame('2020-02-29', $rows[1]['dt']);
        $this->assertSame('1970-01-01T00:00:01.000000Z', $rows[1]['ts']);
        $this->assertSame('01:02:03.004', $rows[1]['tm']);
        $this->assertSame('18446744073709551615', $rows[0]['u'] . '');
    }
}
