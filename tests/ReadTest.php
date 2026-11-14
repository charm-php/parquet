<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Encoding, PhysicalType, Reader, UnsupportedException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Every fixture (written by pyarrow with a choice of codec, page version, dictionary and sizes) against the values pyarrow reads from it. */
final class ReadTest extends TestCase
{
    /** @return array<string, array{string}> */
    public static function fixtures(): array
    {
        $out = [];
        foreach (Fixtures::names() as $n) {
            $out[$n] = [$n];
        }
        return $out;
    }

    #[DataProvider('fixtures')]
    public function testColumnsEqualPyarrowsValues(string $name): void
    {
        $e = Fixtures::expected($name);
        $r = Fixtures::reader($name);
        $this->assertSame($e['rows'], \array_sum(\array_map(static fn ($g) => $g->numRows, $r->metadata()->rowGroups)));
        if (isset($e['unsupported'])) {
            try {
                $r->readRowGroup(0);
                $this->fail('no UnsupportedException');
            } catch (UnsupportedException $x) {
                $this->assertStringContainsString($e['unsupported'], $x->getMessage());
            }
            return;
        }
        if ($e['columns'] === []) {
            $this->addToAssertionCount(1); // (only nested columns: NestedTest compares those)
            return;
        }
        foreach ($e['columns'] as $path => $col) {
            $leaf = $r->schema()->leaf($path);
            $this->assertNotNull($leaf, $path);
            $this->assertSame(PhysicalType::from(self::physicalId($col['physical'])), $leaf->physicalType);
            $got = [];
            for ($g = 0; $g < $r->rowGroupCount(); $g++) {
                foreach (Canonical::column($leaf->physicalType, $r->readColumn($g, $path)) as $v) {
                    $got[] = $v;
                }
            }
            $this->assertCount(\count($col['values']), $got, "$name $path: number of values");
            // (assertSame on 100k-element lists prints huge diffs: find the first difference by hand)
            foreach ($col['values'] as $i => $want) {
                if ($got[$i] !== $want) {
                    $this->fail("$name column $path row $i: got " . \var_export($got[$i], true) . ', pyarrow ' . \var_export($want, true));
                }
            }
            $this->addToAssertionCount(1);
        }
    }

    #[DataProvider('fixtures')]
    public function testRowsAreTheColumnsTransposed(string $name): void
    {
        $e = Fixtures::expected($name);
        $allTopLevelLeaves = \array_filter(Fixtures::reader($name)->schema()->root->children, static fn ($c) => !$c->isLeaf()) === [];
        if (isset($e['unsupported']) || \count($e['columns']) > 40 || !$allTopLevelLeaves) {
            $this->addToAssertionCount(1);
            return;
        }
        $r = Fixtures::reader($name);
        $cols = \array_keys($e['columns']);
        if (\count($cols) < \count($r->schema()->leaves)) {
            $rows = \iterator_to_array($r->rows($cols), false);
        } else {
            $rows = \iterator_to_array($r->rows(), false);
        }
        $this->assertCount($e['rows'], $rows);
        foreach ($rows as $i => $row) {
            $this->assertSame($cols, \array_keys($row));
            foreach ($cols as $c) {
                $leaf = $r->schema()->leaf($c);
                $this->assertSame($e['columns'][$c]['values'][$i], Canonical::value($leaf->physicalType, $row[$c]), "$name row $i column $c");
            }
        }
    }

    private static function physicalId(string $name): int
    {
        return ['BOOLEAN' => 0, 'INT32' => 1, 'INT64' => 2, 'INT96' => 3, 'FLOAT' => 4, 'DOUBLE' => 5, 'BYTE_ARRAY' => 6, 'FIXED_LEN_BYTE_ARRAY' => 7][$name];
    }

    public function testRowsWithColumnSelection(): void
    {
        $r = Fixtures::reader('matrix-snappy-v1-dict');
        $rows = \iterator_to_array($r->rows(['o_string', 'r_int32']), false);
        $this->assertCount(250, $rows);
        $this->assertSame(['o_string', 'r_int32'], \array_keys($rows[0]));
        $cols = $r->readRowGroup(0, ['r_int32', 'o_string']);
        $this->assertSame($cols['r_int32'][17], $rows[17]['r_int32']);
        $this->assertSame($cols['o_string'][17], $rows[17]['o_string']);
    }

    public function testUnknownColumnIsAnInvalidArgument(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Fixtures::reader('matrix-none-v1-dict')->readColumn(0, 'nope');
    }

    public function testUnknownRowGroupIsOutOfRange(): void
    {
        $this->expectException(\OutOfRangeException::class);
        Fixtures::reader('matrix-none-v1-dict')->readColumn(3, 'r_int32');
    }

    public function testNullPatterns(): void
    {
        $r = Fixtures::reader('null-patterns');
        $start = $r->readColumn(0, 'start');
        $this->assertCount(400, $start);
        $this->assertSame(\array_fill(0, 150, null), \array_slice($start, 0, 150));
        $this->assertSame(0, $start[150]);
        $this->assertSame(249, $start[399]);
        $end = $r->readColumn(0, 'end');
        $this->assertSame(249, $end[249]);
        $this->assertSame(\array_fill(0, 150, null), \array_slice($end, 250));
        $this->assertSame(\array_fill(0, 400, null), $r->readColumn(0, 'all_null'));
        $this->assertSame(\array_fill(0, 400, null), $r->readColumn(0, 'all_null_bool'));
        $none = $r->readColumn(0, 'none_null');
        $this->assertNotContains(null, $none);
        $this->assertSame('v0', $none[0]);
        $alt = $r->readColumn(0, 'alternate');
        $this->assertSame(0.0, $alt[0]);
        $this->assertNull($alt[1]);
        $this->assertSame(398.0, $alt[398]);
        $bools = $r->readColumn(0, 'bools');
        $this->assertNull($bools[0]);
        $this->assertSame(false, $bools[1]);
        $this->assertSame(true, $bools[2]);
    }

    public function testEmptyFiles(): void
    {
        $r = Fixtures::reader('empty-no-rowgroups');
        $this->assertSame(0, $r->numRows());
        $this->assertSame([], \iterator_to_array($r->rows()));
        $this->assertSame([], $r->readColumn(0, 'a')); // (pyarrow writes one row group with no rows)
        $this->assertSame(['a', 'b'], \array_map(static fn ($l) => $l->dottedPath, $r->schema()->leaves));

        $z = Fixtures::reader('empty-zero-row-group');
        $this->assertSame(2, $z->numRows());
        $this->assertSame([0, 2, 0], \array_map(static fn ($g) => $g->numRows, $z->metadata()->rowGroups));
        $this->assertSame([], $z->readColumn(0, 'a'));
        $this->assertSame([1, 2], $z->readColumn(1, 'a'));
        $this->assertSame([['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => null]], \iterator_to_array($z->rows(), false));
    }

    public function testMultipleRowGroups(): void
    {
        $r = Fixtures::reader('rowgroups');
        $this->assertSame(3, $r->rowGroupCount());
        $this->assertSame([100, 100, 100], \array_map(static fn ($g) => $g->numRows, $r->metadata()->rowGroups));
        $this->assertCount(300, \iterator_to_array($r->rows(['r_int64']), false));
    }

    public function testMultiplePagesPerChunk(): void
    {
        foreach (['pages-v1-dict', 'pages-v1-plain', 'pages-v2-dict', 'pages-v2-plain'] as $name) {
            $r = Fixtures::readerWithBytes($name);
            foreach (['r_int32', 'o_string', 'r_int64'] as $col) {
                $pages = Fixtures::pages($r, 0, $col);
                $data = \array_filter($pages, static fn ($p) => $p[0] === 'data' || $p[0] === 'data-v2');
                $this->assertGreaterThan(1, \count($data), "$name $col has several data pages");
                $this->assertSame(250, \array_sum(\array_column($data, 2)));
            }
        }
    }

    public function testDictionaryFallbackInTheMiddleOfAChunk(): void
    {
        foreach (['dict-fallback-v1', 'dict-fallback-v2'] as $name) {
            $r = Fixtures::readerWithBytes($name);
            $pages = Fixtures::pages($r, 0, 'word');
            $this->assertSame('dictionary', $pages[0][0], $name);
            $encs = \array_map(static fn ($p) => $p[1], \array_filter(\array_slice($pages, 1), static fn ($p) => $p[0] !== 'dictionary'));
            $dictEnc = [Encoding::RleDictionary->value, Encoding::PlainDictionary->value];
            $this->assertContains(\reset($encs), $dictEnc, "$name starts dictionary-encoded");
            $this->assertSame(Encoding::Plain->value, \end($encs), "$name ends PLAIN (the dictionary overflowed)");
            // and it still reads back right (ReadTest::testColumnsEqualPyarrowsValues compares every value)
            $this->assertCount(1500, $r->readColumn(0, 'word'));
        }
    }

    public function testEveryEncodingAndCodecOfTheMatrixIsUsed(): void
    {
        $seen = [];
        foreach (Fixtures::names() as $n) {
            if (!\str_starts_with($n, 'matrix-')) {
                continue;
            }
            $m = Fixtures::reader($n)->metadata();
            foreach ($m->rowGroups[0]->columns as $c) {
                $seen['codec ' . $c->codec->name] = true;
                foreach ($c->encodings as $e) {
                    $seen['encoding ' . $e->name] = true;
                }
            }
        }
        foreach (['codec Uncompressed', 'codec Snappy', 'codec Gzip', 'codec Zstd', 'encoding Plain', 'encoding Rle'] as $k) {
            $this->assertArrayHasKey($k, $seen);
        }
        $this->assertTrue(isset($seen['encoding RleDictionary']) || isset($seen['encoding PlainDictionary']));
    }

    public function testFlatLeavesUnderGroupsAreReadByDottedPath(): void
    {
        $r = Fixtures::reader('nested');
        $this->assertSame([1, 2, 3], $r->readColumn(0, 'id'));
        // an optional group holding a required leaf, and a required group holding an optional leaf: one level of optionality
        $this->assertSame([1, 2, 3], $r->readColumn(0, 'reqst.x'));
        $this->assertSame([10, null, 30], $r->readColumn(0, 'reqgroup.x'));
        // optional inside optional, and lists and maps: the projected shape of the leaf
        $this->assertSame([1, null, 3], $r->readColumn(0, 'st.x'));
        $this->assertSame(['a', null, null], $r->readColumn(0, 'st.y'));
        $this->assertSame([5, null, null], $r->readColumn(0, 'deep.a.b'));
        $this->assertSame([[1, 2], null, []], $r->readColumn(0, 'lst.list.element'));
        $this->assertSame([['k'], null, []], $r->readColumn(0, 'mp.key_value.key'));
        $this->assertSame([[1], null, []], $r->readColumn(0, 'mp.key_value.value'));
        // and whole groups
        $this->assertSame([['x' => 1, 'y' => 'a'], null, ['x' => 3, 'y' => null]], $r->readColumn(0, 'st'));
        $this->assertSame([['k' => 1], null, []], $r->readColumn(0, 'mp'));
    }

    public function testLimits(): void
    {
        $r = Reader::fromFile(Fixtures::path('matrix-none-v1-plain'), maxChunkValues: 100);
        try {
            $r->readColumn(0, 'r_int32');
            $this->fail('no exception');
        } catch (UnsupportedException $e) {
            $this->assertStringContainsString('limit', $e->getMessage());
        }
        $r = Reader::fromFile(Fixtures::path('matrix-none-v1-plain'), maxPageSize: 100);
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessageMatches('/page size limit/');
        $r->readColumn(0, 'r_int32');
    }
}
