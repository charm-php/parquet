<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Reader, ValueMode};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class StatisticsTest extends TestCase
{
    /** @return array<string, array{string}> */
    public static function fixtures(): array
    {
        $out = [];
        foreach (Fixtures::names() as $n) {
            if (isset(Fixtures::expected($n)['statistics'])) {
                $out[$n] = [$n];
            }
        }
        return $out;
    }

    /** The true min and max (computed by pyarrow from the data, not from the footer) against the decoded footer statistics. */
    #[DataProvider('fixtures')]
    public function testDecodedStatisticsEqualTheTrueMinAndMax(string $name): void
    {
        $want = Fixtures::expected($name)['statistics'];
        $r = Fixtures::logicalReader($name);
        foreach ($want as $key => $w) {
            [$g, $path] = \explode(':', $key, 2);
            $s = $r->statistics((int) $g, $path);
            $this->assertNotNull($s, "$name $key");
            // (parquet-cpp stores a zero minimum as -0.0 and a zero maximum as +0.0 on purpose: compare zeros as equal)
            $norm = static fn ($v) => $v === 0.0 ? 0.0 : $v;
            $this->assertSame($w['min'], Canonical::logical($norm($s->min)), "$name $key min");
            $this->assertSame($w['max'], Canonical::logical($norm($s->max)), "$name $key max");
            $this->assertSame($w['nullCount'], $s->nullCount, "$name $key null count");
        }
        $this->addToAssertionCount(1);
    }

    public function testPhysicalAndLogicalModes(): void
    {
        $p = Fixtures::reader('logical-edges')->statistics(0, 'date');
        $l = Fixtures::logicalReader('logical-edges')->statistics(0, 'date');
        $this->assertSame(-2147483648, $p->min);
        $this->assertSame(2147483647, $p->max);
        $this->assertSame('-5877641-06-23', $l->min);
        $this->assertSame('5881580-07-11', $l->max);
        $d = Fixtures::logicalReader('decimals')->statistics(0, 'p4');
        $this->assertIsString($d->min);
        $this->assertMatchesRegularExpression('/^-\d+\.\d{1}$/', $d->min);
        $u = Fixtures::logicalReader('logical-edges')->statistics(0, 'u64');
        $this->assertSame('18446744073709551615', $u->max);
        $this->assertSame(0, $u->min);
    }

    public function testNoStatistics(): void
    {
        $this->assertNull(Fixtures::reader('empty-no-rowgroups')->metadata()->rowGroups[0]->columns[0]->statistics ?? null);
    }

    private function file(array $stats, int $type = 1, ?array $schemaExtra = null): Reader
    {
        $page = FileBuilder::dataPageV1(\pack('V*', 1, 2, 3), 3);
        $leaf = [[1, Thrift::I32, $type], [3, Thrift::I32, 0], [4, Thrift::BINARY, 'c'], ...($schemaExtra ?? [])];
        \usort($leaf, static fn ($a, $b) => $a[0] <=> $b[0]);
        $bytes = FileBuilder::file([['name' => 'c', 'type' => $type, 'chunk' => $type === 1 ? $page : '', 'statistics' => $stats, 'numValues' => 3]], 3,
            ['schema' => [[[4, Thrift::BINARY, 'schema'], [5, Thrift::I32, 1]], $leaf]]);
        return Reader::fromString($bytes, ValueMode::Physical);
    }

    public function testMinValueIsPreferredAndExactnessIsReported(): void
    {
        $s = $this->file([[1, Thrift::BINARY, \pack('l', 99)], [2, Thrift::BINARY, \pack('l', -99)], [3, Thrift::I64, 7], [4, Thrift::I64, 3],
            [5, Thrift::BINARY, \pack('l', 10)], [6, Thrift::BINARY, \pack('l', -10)], [7, Thrift::BOOL_TRUE, false], [8, Thrift::BOOL_TRUE, true]])->statistics(0, 'c');
        $this->assertSame(-10, $s->min);
        $this->assertSame(10, $s->max);
        $this->assertSame(7, $s->nullCount);
        $this->assertSame(3, $s->distinctCount);
        $this->assertFalse($s->isMaxExact);
        $this->assertTrue($s->isMinExact);
    }

    public function testDeprecatedMinMaxAreUsedForSignedIntegersOnly(): void
    {
        $dep = [[1, Thrift::BINARY, \pack('l', 9)], [2, Thrift::BINARY, \pack('l', -9)]];
        $s = $this->file($dep)->statistics(0, 'c');
        $this->assertSame([-9, 9, null, null], [$s->min, $s->max, $s->isMinExact, $s->isMaxExact]);
        // an unsigned annotation (UINT_32): the old ordering was signed, so it says nothing
        $s = $this->file($dep, 1, [[6, Thrift::I32, 13]])->statistics(0, 'c');
        $this->assertNull($s->min);
        $this->assertNull($s->max);
        // byte strings: same
        $s = $this->file([[1, Thrift::BINARY, 'zzz'], [2, Thrift::BINARY, 'aaa']], 6)->statistics(0, 'c');
        $this->assertNull($s->min);
        $s = $this->file([[5, Thrift::BINARY, 'zzz'], [6, Thrift::BINARY, 'aaa']], 6)->statistics(0, 'c');
        $this->assertSame(['aaa', 'zzz'], [$s->min, $s->max]);
    }

    public function testWrongSizedBytesGiveNull(): void
    {
        $s = $this->file([[5, Thrift::BINARY, "\x01\x02"], [6, Thrift::BINARY, '']])->statistics(0, 'c');
        $this->assertNull($s->max);
        $this->assertNull($s->min);
    }

    public function testNoStatisticsStruct(): void
    {
        $page = FileBuilder::dataPageV1(\pack('V*', 1, 2, 3), 3);
        $r = Reader::fromString(FileBuilder::file([['name' => 'c', 'type' => 1, 'chunk' => $page]], 3));
        $this->assertNull($r->statistics(0, 'c'));
    }
}
