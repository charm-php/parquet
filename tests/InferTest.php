<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Reader, TimeUnit, Type, Writer};
use PHPUnit\Framework\TestCase;

/** Type::infer(): the schema of sample rows, and that writing with it reads back the same rows. */
final class InferTest extends TestCase
{
    /** @param list<array<string, mixed>> $rows */
    private function roundTrip(array $rows): array
    {
        $schema = Type::infer($rows);
        $w = new Writer($schema);
        $out = '';
        foreach ($rows as $row) {
            $out .= $w->encode($row);
        }
        return [$schema, iterator_to_array(Reader::fromString($out . $w->finish())->rows(), false)];
    }

    public function testEveryScalarKind(): void
    {
        $rows = [
            ['b' => true, 'i' => 5, 'f' => 1.5, 's' => 'text', 'y' => "\xff\x00", 'e' => 'é'],
            ['b' => false, 'i' => PHP_INT_MIN, 'f' => -0.0, 's' => '', 'y' => 'ascii', 'e' => null],
            ['b' => null, 'i' => PHP_INT_MAX, 'f' => INF, 's' => null, 'y' => null, 'e' => 'x'],
        ];
        [$schema, $back] = $this->roundTrip($rows);
        $this->assertSame(['bool', 'int64', 'double', 'string', 'bytes', 'string'], array_values(array_map(static fn (Type $t) => $t->kind, $schema)));
        foreach ($schema as $t) {
            $this->assertTrue($t->nullable);
        }
        $this->assertSame(WriterHelper::text($rows), WriterHelper::text($back));
    }

    public function testIntAndFloatMergeToDouble(): void
    {
        [$schema, $back] = $this->roundTrip([['n' => 1], ['n' => 2.5], ['n' => null], ['n' => 3]]);
        $this->assertSame('double', $schema['n']->kind);
        $this->assertSame([1.0, 2.5, null, 3.0], array_column($back, 'n'));
    }

    public function testDateTimeIsTimestampMicrosUtc(): void
    {
        $rows = [['t' => new \DateTimeImmutable('2024-02-29 12:30:45.123456', new \DateTimeZone('UTC'))], ['t' => new \DateTime('1969-12-31 23:59:59', new \DateTimeZone('Europe/Oslo'))]];
        [$schema, $back] = $this->roundTrip($rows);
        $this->assertSame('timestamp', $schema['t']->kind);
        $this->assertSame(TimeUnit::Micros, $schema['t']->params['unit']);
        $this->assertTrue($schema['t']->params['utc']);
        $this->assertSame(['2024-02-29T12:30:45.123456Z', '1969-12-31T22:59:59.000000Z'], array_column($back, 't'));
    }

    public function testNullOnlyAndEmptyOnlyColumnsGetDocumentedDefaults(): void
    {
        [$schema, $back] = $this->roundTrip([['n' => null, 'l' => [], 'k' => 1], ['n' => null, 'l' => [], 'k' => null]]);
        $this->assertSame('string', $schema['n']->kind);
        $this->assertTrue($schema['n']->nullable);
        $this->assertSame('list', $schema['l']->kind);
        $this->assertSame('string', $schema['l']->children[0]->kind);
        $this->assertSame([[], []], array_column($back, 'l'));
        $this->assertSame([null, null], array_column($back, 'n'));
    }

    public function testListsAreMergedOverElementsAndRows(): void
    {
        $rows = [['l' => [1, 2]], ['l' => []], ['l' => [3.5]], ['l' => null], ['l' => [null, 4]]];
        [$schema, $back] = $this->roundTrip($rows);
        $this->assertSame('list', $schema['l']->kind);
        $this->assertSame('double', $schema['l']->children[0]->kind);
        $this->assertSame([[1.0, 2.0], [], [3.5], null, [null, 4.0]], array_column($back, 'l'));
        // a list that is empty in the first row only takes its element type from the later rows
        [$schema] = $this->roundTrip([['l' => []], ['l' => ['a']]]);
        $this->assertSame('string', $schema['l']->children[0]->kind);
        [$schema] = $this->roundTrip([['l' => []], ['l' => [[1]]]]);
        $this->assertSame('list', $schema['l']->children[0]->kind);
    }

    public function testStructsMergeFieldsAndMissingMeansNullable(): void
    {
        $rows = [
            ['p' => ['name' => 'a', 'age' => 3], 'xs' => [['k' => 1], ['k' => 2, 'tags' => ['x']]]],
            ['p' => ['name' => 'b', 'city' => 'Oslo'], 'xs' => []],
            ['p' => null, 'xs' => [['tags' => []]]],
        ];
        [$schema, $back] = $this->roundTrip($rows);
        $this->assertSame('struct', $schema['p']->kind);
        $this->assertSame(['name', 'age', 'city'], $schema['p']->names);
        $this->assertSame(['k', 'tags'], $schema['xs']->children[0]->names);
        $this->assertSame(
            WriterHelper::text(WriterHelper::normalizeRows($schema, $rows)),
            WriterHelper::text($back),
        );
        $this->assertSame(['name' => 'b', 'age' => null, 'city' => 'Oslo'], $back[1]['p']);
    }

    public function testColumnsAreInTheOrderTheyFirstAppear(): void
    {
        $schema = Type::infer([['b' => 1], ['a' => 2, 'b' => 3], ['c' => 4]]);
        $this->assertSame(['b', 'a', 'c'], array_keys($schema));
    }

    public function testAcceptsAGeneratorAndAnyIterable(): void
    {
        $gen = (static function () {
            yield ['a' => 1];
            yield ['a' => 2];
        })();
        $this->assertSame(['a'], array_keys(Type::infer($gen)));
    }

    /** @return array<string, array{list<mixed>, string}> */
    public static function conflicts(): array
    {
        return [
            'int and string' => [[['a' => 1], ['a' => 'x']], "column 'a', row 1: cannot combine int and string"],
            'bool and int' => [[['a' => true], ['a' => 1]], "column 'a', row 1: cannot combine bool and int"],
            'list and scalar' => [[['a' => [1]], ['a' => 1]], "column 'a', row 1: cannot combine a list and int"],
            'list and struct' => [[['a' => [1]], ['a' => ['x' => 1]]], "column 'a', row 1: cannot combine a list and an array with string keys"],
            'nested struct field' => [[['a' => ['x' => 1]], ['a' => ['x' => 'y']]], "column 'a.x', row 1: cannot combine int and string"],
            'list elements' => [[['a' => [1, 'x']]], "column 'a[]', row 0: cannot combine int and string"],
            'list element across rows' => [[['a' => [1]], ['a' => ['x']]], "column 'a[]', row 1: cannot combine int and string"],
            'datetime and string' => [[['a' => new \DateTime('2020-01-01')], ['a' => '2020-01-01']], "cannot combine DateTimeInterface and string"],
            'int keys that are not a list' => [[['a' => [5 => 'x']]], 'integer keys that is not a list'],
        ];
    }

    /** @param list<mixed> $rows */
    #[\PHPUnit\Framework\Attributes\DataProvider('conflicts')]
    public function testConflictsAreValueErrorsNamingThePathAndRow(array $rows, string $message): void
    {
        try {
            Type::infer($rows);
            $this->fail('no error');
        } catch (\ValueError $e) {
            $this->assertStringContainsString($message, $e->getMessage());
        }
    }

    public function testOtherErrors(): void
    {
        try {
            Type::infer([['a' => new \stdClass()]]);
            $this->fail('no error');
        } catch (\TypeError $e) {
            $this->assertStringContainsString("column 'a', row 0", $e->getMessage());
        }
        try {
            Type::infer([['a' => 1], 'x']);
            $this->fail('no error');
        } catch (\TypeError $e) {
            $this->assertStringContainsString('row 1', $e->getMessage());
        }
        foreach ([[], [[]]] as $rows) {
            try {
                Type::infer($rows);
                $this->fail('no error for ' . json_encode($rows));
            } catch (\ValueError $e) {
                $this->assertStringContainsString('Cannot infer', $e->getMessage());
            }
        }
        $this->assertSame(['string'], array_map(static fn (Type $t) => $t->kind, array_values(Type::infer([['a' => null]]))));
    }

    public function testNeverInfersDecimalsDatesOrMaps(): void
    {
        $schema = Type::infer([['d' => '1.50', 'day' => '2024-01-01', 'f' => 1.5, 'm' => ['a' => 1]]]);
        $this->assertSame('string', $schema['d']->kind);
        $this->assertSame('string', $schema['day']->kind);
        $this->assertSame('double', $schema['f']->kind);
        $this->assertSame('struct', $schema['m']->kind);
    }
}
