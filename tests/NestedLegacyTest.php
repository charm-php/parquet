<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{ParquetException, MapMode, Reader, UnsupportedException, ValueMode};
use PHPUnit\Framework\TestCase;

/**
 * The backward-compatibility forms of lists and maps and the shapes pyarrow does not write, built by hand from shredded levels
 * with NestedFileBuilder; rows that span data pages; and hostile level streams.
 *
 * Each rule of the Parquet spec ("Backward-compatibility rules", LogicalTypes.md) and where it is exercised:
 *   rule 1 (a repeated primitive in a LIST group is the element)            testRepeatedPrimitiveInsideAListGroup; corpus: old_list_structure? (see the report)
 *   rule 2 (a repeated group with several fields is the element struct)     testTwoLevelListOfStructs; corpus: nonnullable.impala, nullable.impala (Int_Array...)
 *   rule 3 (a single-field repeated group named "array" or "<list>_tuple")  testSingleFieldGroupNamedArray, testSingleFieldGroupNamedTuple
 *   rule 3 exception (the repeated group is itself annotated LIST/MAP)      testOldStyleListOfLists; corpus: old_list_structure.parquet (a.array.array)
 *   rule 4 (otherwise the repeated group is the 3-level wrapper)            testThreeLevelListWithARequiredElement and every pyarrow fixture
 *   unannotated repeated primitive / group (a list outside of LIST)        testUnannotatedRepeatedPrimitive/Group; corpus: repeated_primitive_no_list, repeated_no_annotation
 *   MAP_KEY_VALUE legacy annotation, and a map without a value (a set)      testLegacyMapKeyValue, testMapWithoutValue; corpus: map_no_value.parquet
 */
final class NestedLegacyTest extends TestCase
{
    private const R = NestedFileBuilder::REQ;
    private const O = NestedFileBuilder::OPT;
    private const P = NestedFileBuilder::REP;

    private static function leaf(string $n, int $rep, int $type = 1, ?int $conv = null): array
    {
        return NestedFileBuilder::leaf($n, $rep, $type, $conv);
    }

    private static function group(string $n, int $rep, array $c, ?int $conv = null): array
    {
        return NestedFileBuilder::group($n, $rep, $c, $conv);
    }

    private function rows(string $bytes, ValueMode $m = ValueMode::Physical, MapMode $mm = MapMode::Auto): array
    {
        return \iterator_to_array(Reader::fromString($bytes, $m, mapMode: $mm)->rows(), false);
    }

    public function testRepeatedPrimitiveInsideAListGroup(): void
    {
        // optional group l (LIST) { repeated int32 element; }  rows: [1,2], null, [], [3]
        $f = NestedFileBuilder::file([self::group('l', self::O, [self::leaf('element', self::P)], NestedFileBuilder::CONV_LIST)],
            [['values' => [1, 2, 3], 'defs' => [2, 2, 0, 1, 2], 'reps' => [0, 1, 0, 0, 0]]], 4);
        $this->assertSame([['l' => [1, 2]], ['l' => null], ['l' => []], ['l' => [3]]], $this->rows($f));
    }

    public function testTwoLevelListOfStructs(): void
    {
        // optional group l (LIST) { repeated group element { required int32 a; required int32 b; } }  rows: [{1,2},{3,4}], [], null
        $f = NestedFileBuilder::file([self::group('l', self::O, [self::group('element', self::P, [self::leaf('a', self::R), self::leaf('b', self::R)])], NestedFileBuilder::CONV_LIST)],
            [['values' => [1, 3], 'defs' => [2, 2, 1, 0], 'reps' => [0, 1, 0, 0]], ['values' => [2, 4], 'defs' => [2, 2, 1, 0], 'reps' => [0, 1, 0, 0]]], 3);
        $this->assertSame([['l' => [['a' => 1, 'b' => 2], ['a' => 3, 'b' => 4]]], ['l' => []], ['l' => null]], $this->rows($f));
    }

    public function testSingleFieldGroupNamedArray(): void
    {
        // optional group l (LIST) { repeated group array { required int32 x; } }: the repeated group IS the element: a list of structs
        $f = NestedFileBuilder::file([self::group('l', self::O, [self::group('array', self::P, [self::leaf('x', self::R)])], NestedFileBuilder::CONV_LIST)],
            [['values' => [5], 'defs' => [2, 1], 'reps' => [0, 0]]], 2);
        $this->assertSame([['l' => [['x' => 5]]], ['l' => []]], $this->rows($f));
    }

    public function testSingleFieldGroupNamedTuple(): void
    {
        // optional group foo (LIST) { repeated group foo_tuple { required int32 y; } }
        $f = NestedFileBuilder::file([self::group('foo', self::O, [self::group('foo_tuple', self::P, [self::leaf('y', self::R)])], NestedFileBuilder::CONV_LIST)],
            [['values' => [8, 9], 'defs' => [2, 2], 'reps' => [0, 1]]], 1);
        $this->assertSame([['foo' => [['y' => 8], ['y' => 9]]]], $this->rows($f));
    }

    public function testOldStyleListOfLists(): void
    {
        // optional group a (LIST) { repeated group array (LIST) { repeated int32 array; } }: the repeated group is itself a list: a list of lists
        // rows: [[1,2],[3,4]], null, [], [[]]
        $f = NestedFileBuilder::file([self::group('a', self::O, [self::group('array', self::P, [self::leaf('array', self::P)], NestedFileBuilder::CONV_LIST)], NestedFileBuilder::CONV_LIST)],
            [['values' => [1, 2, 3, 4], 'defs' => [3, 3, 3, 3, 0, 1, 2], 'reps' => [0, 2, 1, 2, 0, 0, 0]]], 4);
        $this->assertSame([['a' => [[1, 2], [3, 4]]], ['a' => null], ['a' => []], ['a' => [[]]]], $this->rows($f));
    }

    public function testThreeLevelListWithARequiredElement(): void
    {
        // optional group l (LIST) { repeated group list { required int32 element; } }: a single field with another name: the wrapper
        $f = NestedFileBuilder::file([self::group('l', self::O, [self::group('list', self::P, [self::leaf('element', self::R)])], NestedFileBuilder::CONV_LIST)],
            [['values' => [7, 8], 'defs' => [2, 2, 1], 'reps' => [0, 1, 0]]], 2);
        $this->assertSame([['l' => [7, 8]], ['l' => []]], $this->rows($f));
    }

    public function testUnannotatedRepeatedPrimitive(): void
    {
        // repeated int32 r; rows: [1,2], [], [3]
        $f = NestedFileBuilder::file([self::leaf('r', self::P)], [['values' => [1, 2, 3], 'defs' => [1, 1, 0, 1], 'reps' => [0, 1, 0, 0]]], 3);
        $this->assertSame([['r' => [1, 2]], ['r' => []], ['r' => [3]]], $this->rows($f));
        $r = Reader::fromString($f);
        $this->assertSame([[1, 2], [], [3]], $r->readColumn(0, 'r')); // a repeated leaf is not "flat": it goes through the assembler
    }

    public function testUnannotatedRepeatedGroup(): void
    {
        // repeated group g { required int32 a; optional int32 b; }  rows: [{1,null},{2,5}], []
        $f = NestedFileBuilder::file([self::group('g', self::P, [self::leaf('a', self::R), self::leaf('b', self::O)])],
            [['values' => [1, 2], 'defs' => [1, 1, 0], 'reps' => [0, 1, 0]], ['values' => [5], 'defs' => [1, 2, 0], 'reps' => [0, 1, 0]]], 2);
        $this->assertSame([['g' => [['a' => 1, 'b' => null], ['a' => 2, 'b' => 5]]], ['g' => []]], $this->rows($f));
    }

    public function testLegacyMapKeyValue(): void
    {
        // optional group m (MAP) { repeated group map (MAP_KEY_VALUE) { required binary key (UTF8); optional int32 value; } }
        // rows: {a: 1, b: null}, null, []
        $f = NestedFileBuilder::file(
            [self::group('m', self::O, [self::group('map', self::P, [self::leaf('key', self::R, 6, NestedFileBuilder::CONV_UTF8), self::leaf('value', self::O)], NestedFileBuilder::CONV_MAP_KEY_VALUE)], NestedFileBuilder::CONV_MAP)],
            [['values' => ['a', 'b'], 'defs' => [2, 2, 0, 1], 'reps' => [0, 1, 0, 0]], ['values' => [1], 'defs' => [3, 2, 0, 1], 'reps' => [0, 1, 0, 0]]], 3);
        $this->assertSame([['m' => ['a' => 1, 'b' => null]], ['m' => null], ['m' => []]], $this->rows($f));
        $this->assertSame([['m' => [['a', 1], ['b', null]]], ['m' => null], ['m' => []]], $this->rows($f, mm: MapMode::Pairs));
    }

    public function testMapWithoutValue(): void
    {
        // optional group m (MAP) { repeated group key_value { required binary key; } }: a set: key => null
        $f = NestedFileBuilder::file(
            [self::group('m', self::O, [self::group('key_value', self::P, [self::leaf('key', self::R, 6)])], NestedFileBuilder::CONV_MAP)],
            [['values' => ['x', 'y'], 'defs' => [2, 2], 'reps' => [0, 1]]], 1);
        $this->assertSame([['m' => ['x' => null, 'y' => null]]], $this->rows($f));
        $this->assertSame([['m' => [['x', null], ['y', null]]]], $this->rows($f, mm: MapMode::Pairs));
    }

    public function testRowsSpanDataPagesInBothPageVersions(): void
    {
        // optional group l (LIST) { repeated group list { optional int32 element; } }: [1,2,3,4], [5], [], [6,7]
        $data = ['values' => [1, 2, 3, 4, 5, 6, 7], 'defs' => [3, 3, 3, 3, 3, 1, 3, 3], 'reps' => [0, 1, 1, 1, 0, 0, 0, 1]];
        foreach ([1, 2] as $version) {
            foreach ([[3, 3, 2], [1, 1, 1, 1, 1, 1, 1, 1], [5, 3], [8]] as $split) {
                $f = NestedFileBuilder::file([self::group('l', self::O, [self::group('list', self::P, [self::leaf('element', self::O)])], NestedFileBuilder::CONV_LIST)],
                    [$data + ['split' => $split, 'version' => $version]], 4);
                $this->assertSame([['l' => [1, 2, 3, 4]], ['l' => [5]], ['l' => []], ['l' => [6, 7]]], $this->rows($f), "version $version, pages " . \implode(',', $split));
            }
        }
    }

    // ---- hostile level streams ----

    /** @return array<string, array{callable(): string}> */
    public static function hostile(): array
    {
        $list = static fn (array $data, int $rows, array $opts = []): string => NestedFileBuilder::file(
            [NestedFileBuilder::group('l', NestedFileBuilder::OPT, [NestedFileBuilder::group('list', NestedFileBuilder::REP, [NestedFileBuilder::leaf('element', NestedFileBuilder::OPT)])], NestedFileBuilder::CONV_LIST)],
            [$data], $rows, $opts);
        $two = static fn (array $a, array $b, int $rows): string => NestedFileBuilder::file(
            [NestedFileBuilder::group('l', NestedFileBuilder::OPT, [NestedFileBuilder::group('element', NestedFileBuilder::REP, [NestedFileBuilder::leaf('a', NestedFileBuilder::REQ), NestedFileBuilder::leaf('b', NestedFileBuilder::REQ)])], NestedFileBuilder::CONV_LIST)],
            [$a, $b], $rows);
        return [
            'definition level above the maximum' => [fn () => $list(['values' => [1], 'defs' => [9], 'reps' => [0]], 1)],
            'repetition level above the maximum' => [fn () => $list(['values' => [1, 2], 'defs' => [3, 3], 'reps' => [0, 5]], 1)],
            'chunk starts with repetition level 1' => [fn () => $list(['values' => [1, 2], 'defs' => [3, 3], 'reps' => [1, 1]], 1)],
            'repetition levels never return to 0 (3 rows claimed, 1 record)' => [fn () => $list(['values' => [1, 2, 3], 'defs' => [3, 3, 3], 'reps' => [0, 1, 1]], 3)],
            'more records than rows' => [fn () => $list(['values' => [1, 2], 'defs' => [3, 3], 'reps' => [0, 0]], 1)],
            'a repetition level that continues a list that was empty' => [fn () => $list(['values' => [1], 'defs' => [1, 3], 'reps' => [0, 1]], 1)],
            'a repetition level with a definition level below the element' => [fn () => $list(['values' => [1], 'defs' => [3, 1], 'reps' => [0, 1]], 1)],
            'fewer values than the definition levels say' => [fn () => $list(['values' => [1], 'defs' => [3, 3, 3], 'reps' => [0, 1, 1]], 1)],
            'num_values larger than the levels of the page' => [fn () => $list(['values' => [1, 2], 'defs' => [3, 3], 'reps' => [0, 1]], 1, ['numValuesOverride' => [9]])],
            'num_values smaller than the levels of the page' => [fn () => $list(['values' => [1, 2], 'defs' => [3, 3], 'reps' => [0, 1]], 1, ['numValuesOverride' => [1]])],
            'sibling leaves with different list lengths' => [fn () => $two(
                ['values' => [1, 2, 3], 'defs' => [2, 2, 2], 'reps' => [0, 1, 1]], ['values' => [4, 5], 'defs' => [2, 2], 'reps' => [0, 1]], 1)],
            'sibling leaves with the same number of elements but different list lengths' => [fn () => $two(
                ['values' => [1, 2, 3], 'defs' => [2, 2, 2], 'reps' => [0, 1, 0]], ['values' => [4, 5, 6], 'defs' => [2, 2, 2], 'reps' => [0, 0, 1]], 2)],
            'sibling leaves with different numbers of rows' => [fn () => $two(
                ['values' => [1, 2], 'defs' => [2, 2], 'reps' => [0, 0]], ['values' => [4], 'defs' => [2], 'reps' => [0]], 2)],
            'a column of 2^40 entries (the allocation is refused, not attempted)' => [fn () => $list(['values' => [1], 'defs' => [3], 'reps' => [0]], 1, ['numValuesOverride' => [1 << 40]])],
            'a row group of 2^40 rows' => [fn () => $list(['values' => [1], 'defs' => [3], 'reps' => [0]], 1, ['rowsOverride' => 1 << 40])],
        ];
    }

    /** @dataProvider hostile */
    #[\PHPUnit\Framework\Attributes\DataProvider('hostile')]
    public function testHostileLevelStreamsAreRefused(callable $make): void
    {
        $bytes = $make();
        \set_error_handler(static function (int $no, string $msg, string $file, int $line): never {
            throw new \ErrorException($msg, 0, $no, $file, $line);
        });
        $t = \hrtime(true);
        try {
            try {
                \iterator_to_array(Reader::fromString($bytes)->rows());
                $this->fail('the file was read');
            } catch (ParquetException) {
                $this->addToAssertionCount(1);
            }
        } finally {
            \restore_error_handler();
        }
        $this->assertLessThan(2000, (\hrtime(true) - $t) / 1e6);
    }

    /** The shredded accessor validates on its own (it does not go through the assembler's checks). */
    public function testShreddedAccessorRefusesRecordsThatDoNotAddUp(): void
    {
        $list = static fn (array $data, int $rows): string => NestedFileBuilder::file(
            [NestedFileBuilder::group('l', NestedFileBuilder::OPT, [NestedFileBuilder::group('list', NestedFileBuilder::REP, [NestedFileBuilder::leaf('element', NestedFileBuilder::OPT)])], NestedFileBuilder::CONV_LIST)],
            [$data], $rows);
        foreach ([
            'starts with repetition level 1' => $list(['values' => [1, 2], 'defs' => [3, 3], 'reps' => [1, 1]], 1),
            'two records in one row' => $list(['values' => [1, 2], 'defs' => [3, 3], 'reps' => [0, 0]], 1),
            'one record in three rows' => $list(['values' => [1, 2, 3], 'defs' => [3, 3, 3], 'reps' => [0, 1, 1]], 3),
        ] as $what => $bytes) {
            try {
                Reader::fromString($bytes)->readShredded(0, 'l.list.element');
                $this->fail("accepted: $what");
            } catch (ParquetException) {
                $this->addToAssertionCount(1);
            }
        }
        $ok = $list(['values' => [1, 2], 'defs' => [3, 3, 1, 0], 'reps' => [0, 1, 0, 0]], 3);
        $sc = Reader::fromString($ok)->readShredded(0, 'l.list.element');
        $this->assertSame([1, 2], $sc->values);
        $this->assertSame([3, 3, 1, 0], $sc->definitionLevels);
        $this->assertSame([0, 1, 0, 0], $sc->repetitionLevels);
    }

    public function testTheBuilderItselfProducesReadableFilesForTheSameShapes(): void
    {
        $f = NestedFileBuilder::file(
            [NestedFileBuilder::group('l', NestedFileBuilder::OPT, [NestedFileBuilder::group('list', NestedFileBuilder::REP, [NestedFileBuilder::leaf('element', NestedFileBuilder::OPT)])], NestedFileBuilder::CONV_LIST)],
            [['values' => [1, 2], 'defs' => [3, 3], 'reps' => [0, 1]]], 1);
        $this->assertSame([['l' => [1, 2]]], $this->rows($f));
    }
}
