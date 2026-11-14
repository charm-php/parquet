<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{MapMode, Reader, ValueMode};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Nested columns written by pyarrow (lists, structs, maps, required and optional at every level, data pages v1 and v2, tiny pages)
 * against pyarrow's own reading of the same files, converted to this library's shapes by tests/tools/oracle.py (expected/*.json "trees").
 */
final class NestedTest extends TestCase
{
    /** @return array<string, array{string}> */
    public static function fixtures(): array
    {
        $out = [];
        foreach (Fixtures::names() as $n) {
            if (isset(Fixtures::expected($n)['trees'])) {
                $out[$n] = [$n];
            }
        }
        return $out;
    }

    /** @param list<array<string, mixed>> $rows */
    private function compare(string $name, array $rows, array $want, string $what): void
    {
        $this->assertSame(\array_keys($want), $rows === [] ? \array_keys($want) : \array_keys($rows[0]), "$name ($what): top-level names");
        foreach ($want as $top => $expected) {
            $this->assertCount(\count($expected), $rows, "$name ($what) $top: rows");
            foreach ($expected as $i => $w) {
                $got = Canonical::tree($rows[$i][$top]);
                if ($got !== $w) {
                    $this->fail("$name ($what) field $top row $i:\n got      " . \substr(\json_encode($got), 0, 400) . "\n pyarrow  " . \substr(\json_encode($w), 0, 400));
                }
            }
        }
        $this->addToAssertionCount(1);
    }

    #[DataProvider('fixtures')]
    public function testRowsEqualPyarrowsInBothValueModesAndBothMapModes(string $name): void
    {
        $trees = Fixtures::expected($name)['trees'];
        foreach (['physical' => [ValueMode::Physical, MapMode::Auto], 'logical' => [ValueMode::Logical, MapMode::Auto], 'pairs' => [ValueMode::Logical, MapMode::Pairs]] as $key => [$vm, $mm]) {
            $r = Reader::fromFile(Fixtures::path($name), $vm, mapMode: $mm);
            $rows = \iterator_to_array($r->rows(), false);
            $this->compare($name, $rows, $trees[$key], $key);
        }
    }

    #[DataProvider('fixtures')]
    public function testReadRowGroupAndReadColumnAgreeWithRows(string $name): void
    {
        $r = Fixtures::logicalReader($name);
        if ($r->rowGroupCount() === 0) {
            $this->addToAssertionCount(1);
            return;
        }
        $rows = \iterator_to_array($r->rows(), false);
        $off = 0;
        for ($g = 0; $g < $r->rowGroupCount(); $g++) {
            $rg = $r->readRowGroup($g);
            foreach ($rg as $top => $col) {
                $this->assertSame(\array_column(\array_slice($rows, $off, \count($col)), $top), $col, "$name row group $g field $top");
                $this->assertEqualsCanonicalizing($col, $r->readColumn($g, $top));
            }
            $off += \count(\reset($rg));
        }
    }

    // ---- projected leaves are derivable from the assembled values (which are checked against pyarrow above) ----

    private function pairsReader(string $name): Reader
    {
        return Reader::fromFile(Fixtures::path($name), ValueMode::Physical, mapMode: MapMode::Pairs);
    }

    public function testProjectedLeavesOfLists(): void
    {
        $r = Fixtures::reader('nest-list-v1');
        $this->assertSame($r->readColumn(0, 'li'), $r->readColumn(0, 'li.list.element'));
        $this->assertSame($r->readColumn(0, 'ls'), $r->readColumn(0, 'ls.list.element'));
        $this->assertSame($r->readColumn(0, 'lli'), $r->readColumn(0, 'lli.list.element.list.element'));
        $this->assertSame($r->readColumn(0, 'li_req'), $r->readColumn(0, 'li_req.list.element'));
    }

    public function testProjectedLeafInsideAListOfStructs(): void
    {
        foreach (['nest-list-struct-v1', 'nest-list-struct-v2'] as $name) {
            $r = Fixtures::reader($name);
            $ls = $r->readColumn(0, 'ls');
            $a = $r->readColumn(0, 'ls.list.element.a');
            $b = $r->readColumn(0, 'ls.list.element.b.list.element');
            $this->assertCount(\count($ls), $a);
            foreach ($ls as $i => $list) {
                if ($list === null) {
                    $this->assertNull($a[$i]);
                    $this->assertNull($b[$i]);
                    continue;
                }
                $this->assertSame(\array_map(static fn ($e) => $e === null ? null : $e['a'], $list), $a[$i], "$name row $i: a");
                $this->assertSame(\array_map(static fn ($e) => $e === null ? null : $e['b'], $list), $b[$i], "$name row $i: b");
            }
        }
    }

    public function testProjectedLeavesOfMaps(): void
    {
        $p = $this->pairsReader('nest-map-v2');
        $m = $p->readColumn(0, 'm');
        $keys = $p->readColumn(0, 'm.key_value.key');
        $vals = $p->readColumn(0, 'm.key_value.value');
        foreach ($m as $i => $map) {
            $this->assertSame($map === null ? null : \array_map(static fn ($kv) => $kv[0], $map), $keys[$i]);
            $this->assertSame($map === null ? null : \array_map(static fn ($kv) => $kv[1], $map), $vals[$i]);
        }
    }

    public function testProjectedLeafAtDepthSix(): void
    {
        $r = Fixtures::reader('nest-deep-v1');
        $d = $r->readColumn(0, 'd');
        $got = $r->readColumn(0, 'd.a.b.list.element.c.list.element.d');
        foreach ($d as $i => $row) {
            // d.a.b: list of structs with c: list of structs with d
            $want = null;
            if ($row !== null && $row['a'] !== null && $row['a']['b'] !== null) {
                $want = [];
                foreach ($row['a']['b'] as $el) {
                    $want[] = $el === null || $el['c'] === null ? null : \array_map(static fn ($x) => $x === null ? null : $x['d'], $el['c']);
                }
            }
            $this->assertSame($want, $got[$i], "row $i");
        }
    }

    // ---- selection ----

    public function testSelectingSomeLeavesOfAStructGivesAPartialStruct(): void
    {
        $r = Fixtures::reader('nest-struct-v1');
        $all = \iterator_to_array($r->rows(['s', 'ss', 'id']), false);
        $part = \iterator_to_array($r->rows(['s.b', 'id', 'ss.c.d', 'ss.f']), false);
        $this->assertSame(['s', 'id', 'ss'], \array_keys($part[0]));
        foreach ($all as $i => $row) {
            $this->assertSame($row['id'], $part[$i]['id']);
            $this->assertSame($row['s'] === null ? null : ['b' => $row['s']['b']], $part[$i]['s']);
            $ss = $row['ss'];
            $want = $ss === null ? null : ['c' => $ss['c'] === null ? null : ['d' => $ss['c']['d']], 'f' => $ss['f']];
            $this->assertSame($want, $part[$i]['ss'], "row $i");
        }
        // a group path selects every leaf below it
        $whole = \iterator_to_array($r->rows(['ss']), false);
        foreach ($all as $i => $row) {
            $this->assertSame(['ss' => $row['ss']], $whole[$i]);
        }
        // rows are keyed by top-level names, in the order of the selection
        $this->assertSame(['id', 'ss'], \array_keys(\iterator_to_array($r->rows(['id', 'ss.f']), false)[0]));
    }

    public function testSelectingPartOfAListOfStructs(): void
    {
        $r = Fixtures::reader('nest-list-struct-v2');
        $all = \iterator_to_array($r->rows(['ls']), false);
        $part = \iterator_to_array($r->rows(['ls.list.element.a']), false);
        foreach ($all as $i => $row) {
            $want = $row['ls'] === null ? null : \array_map(static fn ($e) => $e === null ? null : ['a' => $e['a']], $row['ls']);
            $this->assertSame($want, $part[$i]['ls'], "row $i");
        }
    }

    public function testSelectingOnlyTheKeysOrOnlyTheValuesOfAMap(): void
    {
        $r = Reader::fromFile(Fixtures::path('nest-map-v1'), ValueMode::Logical);
        $m = $r->readColumn(0, 'm');
        $keysOnly = \iterator_to_array($r->rows(['m.key_value.key']), false);
        foreach ($m as $i => $map) {
            $this->assertSame($map === null ? null : \array_map(static fn ($k) => null, $map), $keysOnly[$i]['m'], "row $i"); // (a set: key => null)
        }
    }

    public function testUnknownPathsAreInvalidArguments(): void
    {
        $r = Fixtures::reader('nest-struct-v1');
        foreach ([fn () => $r->readColumn(0, 's.nope'), fn () => \iterator_to_array($r->rows(['nope'])), fn () => $r->readRowGroup(0, ['s.zzz'])] as $call) {
            try {
                $call();
                $this->fail('accepted');
            } catch (\InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    // ---- maps ----

    public function testMapShapes(): void
    {
        $r = Reader::fromFile(Fixtures::path('nest-map-v1'), ValueMode::Logical);
        $rows = \iterator_to_array($r->rows(), false);
        // string keys: an associative array; numeric-string keys become int keys; a duplicate key keeps its last value (in its first position)
        $this->assertSame(['a' => 3, 'b' => 2], $rows[0]['m_dup']);
        $this->assertSame(['z' => 1], $rows[1]['m_dup']);
        foreach ($rows as $row) {
            foreach ((array) $row['m_numeric_keys'] as $k => $_) {
                $this->assertIsInt($k);
                $this->assertSame(0, $k % 3);
            }
        }
        // a sequential int key map is an associative array that happens to be a list
        foreach ($rows as $row) {
            $this->assertTrue(\array_is_list($row['m_seq']) || \array_keys($row['m_seq']) === \range(0, \count($row['m_seq']) - 1));
        }
        // bool and double keys: pairs
        foreach ($rows as $row) {
            foreach ([$row['m_bool'], $row['m_double']] as $m) {
                foreach ($m ?? [] as $pair) {
                    $this->assertIsArray($pair);
                    $this->assertCount(2, $pair);
                }
            }
        }
        // date and decimal keys are strings in logical mode: associative arrays
        foreach ($rows as $row) {
            foreach (\array_keys($row['m_date']) as $k) {
                $this->assertMatchesRegularExpression('/^\d{4}-\d\d-\d\d$/', (string) $k);
            }
        }
        $pairs = Reader::fromFile(Fixtures::path('nest-map-v1'), ValueMode::Logical, mapMode: MapMode::Pairs);
        $prow = \iterator_to_array($pairs->rows(['m_dup']), false);
        $this->assertSame([['a', 1], ['b', 2], ['a', 3]], $prow[0]['m_dup']); // pairs keep everything
    }

    public function testMapShapeDependsOnTheSchemaNotOnTheValues(): void
    {
        // an empty map and a null map in a column whose key type makes pairs: still [] and null
        $r = Reader::fromFile(Fixtures::path('nest-map-v1'), ValueMode::Logical);
        $rows = \iterator_to_array($r->rows(['m_bool', 'm_int']), false);
        $sawEmpty = false;
        $sawNull = false;
        foreach ($rows as $row) {
            $sawEmpty = $sawEmpty || $row['m_bool'] === [];
            $sawNull = $sawNull || $row['m_bool'] === null;
        }
        $this->assertTrue($sawEmpty && $sawNull);
    }

    // ---- the shredded form ----

    public function testShreddedLevelsAreWhatTheSpecSaysForAnOptionalListOfOptionalInts(): void
    {
        foreach (['nest-list-v1', 'nest-list-v2'] as $name) {
            $r = Fixtures::reader($name);
            $li = $r->readColumn(0, 'li');
            $sc = $r->readShredded(0, 'li.list.element');
            $this->assertSame(3, $sc->leaf->maxDefinitionLevel);
            $this->assertSame(1, $sc->leaf->maxRepetitionLevel);
            $defs = [];
            $reps = [];
            $vals = [];
            foreach ($li as $list) {
                if ($list === null) {
                    $defs[] = 0;
                    $reps[] = 0;
                } elseif ($list === []) {
                    $defs[] = 1;
                    $reps[] = 0;
                } else {
                    foreach ($list as $k => $e) {
                        $defs[] = $e === null ? 2 : 3;
                        $reps[] = $k === 0 ? 0 : 1;
                        if ($e !== null) {
                            $vals[] = $e;
                        }
                    }
                }
            }
            $this->assertSame($defs, $sc->definitionLevels, "$name definition levels");
            $this->assertSame($reps, $sc->repetitionLevels, "$name repetition levels");
            $this->assertSame($vals, $sc->values, "$name values");
        }
    }

    public function testShreddedColumnOfARequiredLeafHasNoLevels(): void
    {
        $sc = Fixtures::reader('nest-struct-v1')->readShredded(0, 'req.x'); // required leaf in an optional struct: definition levels, no repetition
        $this->assertNull($sc->repetitionLevels);
        $this->assertSame(\array_fill(0, 120, 1), $sc->definitionLevels);
        $this->assertCount(120, $sc->values);
        $sc = Fixtures::reader('nest-list-v1')->readShredded(0, 'li_req.list.element'); // required list of required elements: levels 0..1 only if optional list
        $this->assertSame(1, $sc->leaf->maxRepetitionLevel);
        $this->assertNull(Fixtures::reader('empty-no-rowgroups')->readShredded(0, 'a')->repetitionLevels);
    }

    // ---- sizes ----

    public function testALargeListAndEmptyListsAndZeroRows(): void
    {
        $r = Fixtures::reader('nest-large-list');
        $rows = $r->readColumn(0, 'l');
        $this->assertSame([], $rows[0]);
        $this->assertNull($rows[1]);
        $this->assertCount(100000, $rows[2]);
        $this->assertSame(999, $rows[2][999]);
        $this->assertSame(0, $rows[2][1000]);
        $this->assertSame(999, $rows[2][99999]);
        $this->assertSame([1, null, 3], $rows[3]);
        $this->assertSame([], $rows[4]);

        foreach (['nest-empty-lists-v1', 'nest-empty-lists-v2'] as $name) {
            $r = Fixtures::reader($name);
            foreach (\iterator_to_array($r->rows(), false) as $row) {
                $this->assertSame(['l' => [], 'ls' => []], $row);
            }
        }
        $r = Fixtures::reader('nest-zero-rows');
        $this->assertSame([], \iterator_to_array($r->rows()));
        for ($g = 0; $g < $r->rowGroupCount(); $g++) {
            $this->assertSame([], $r->readColumn($g, 'l'));
            $this->assertSame([], $r->readColumn($g, 'm'));
            $this->assertSame([], $r->readColumn($g, 's.b'));
        }
    }
}
