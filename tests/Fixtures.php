<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Reader, ValueMode};

/** The pyarrow-made fixtures of tests/fixtures (see generate.py) and their expected values. */
final class Fixtures
{
    public static function path(string $name): string
    {
        return __DIR__ . "/fixtures/$name.parquet";
    }

    public static function bytes(string $name): string
    {
        return (string) \file_get_contents(self::path($name));
    }

    /** @return list<string> */
    public static function names(): array
    {
        $names = [];
        foreach (\glob(__DIR__ . '/fixtures/expected/*.json') ?: [] as $f) {
            $names[] = \basename($f, '.json');
        }
        \sort($names);
        return $names;
    }

    /** @return array<string, mixed> the decoded expected JSON, with "columns" filled in from the file it shares its values with */
    public static function expected(string $name): array
    {
        $e = \json_decode((string) \file_get_contents(__DIR__ . "/fixtures/expected/$name.json"), true, 512, \JSON_THROW_ON_ERROR);
        if (isset($e['sameValuesAs'])) {
            $other = self::expected($e['sameValuesAs']);
            $e['rows'] = $other['rows'];
            $e['columns'] = $other['columns'];
            foreach (['trees', 'columnsLogical'] as $k) {
                if (isset($other[$k])) {
                    $e[$k] = $other[$k];
                }
            }
        }
        return $e;
    }

    /** A reader on the fixture, returning physical values (the expected JSON files hold physical values). */
    public static function reader(string $name): Reader
    {
        return Reader::fromFile(self::path($name), ValueMode::Physical);
    }

    public static function logicalReader(string $name): Reader
    {
        return Reader::fromFile(self::path($name), ValueMode::Logical);
    }

    /**
     * Every page of a column chunk, as [type, encoding, numValues, compressedSize], by walking the page headers.
     *
     * @return list<array{string, ?int, int, int}>
     */
    public static function pages(Reader $r, int $rowGroup, string $column): array
    {
        $meta = $r->metadata()->rowGroups[$rowGroup]->columns[$r->schema()->leaf($column)->leafIndex];
        $bytes = $r->metadata() !== null ? self::bytesOf($r) : '';
        $pos = $meta->startOffset();
        $end = $pos + $meta->totalCompressedSize;
        $pages = [];
        while ($pos < $end) {
            $t = new \Charm\Parquet\Thrift\CompactReader($bytes, $pos);
            $h = $t->readStruct();
            $pos = $t->position() + $h[3];
            $type = [0 => 'data', 1 => 'index', 2 => 'dictionary', 3 => 'data-v2'][$h[1]];
            $enc = isset($h[5]) ? $h[5][2] : (isset($h[7]) ? $h[7][2] : ($h[8][4] ?? null));
            $n = isset($h[5]) ? $h[5][1] : (isset($h[7]) ? $h[7][1] : ($h[8][1] ?? 0));
            $pages[] = [$type, $enc, $n, $h[3]];
        }
        return $pages;
    }

    private static string $last = '';

    private static function bytesOf(Reader $r): string
    {
        return self::$last;
    }

    /** Like reader(), remembering the file bytes for pages(). */
    public static function readerWithBytes(string $name): Reader
    {
        self::$last = self::bytes($name);
        return Reader::fromString(self::$last, ValueMode::Physical);
    }
}
