<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Reader, Type, ValueMode, Writer};

/** Shared by the writer tests and tools: normalising expected rows, writing, reading back. */
final class WriterHelper
{
    /** Rows as Reader::rows() returns them: every top-level key present, every missing struct field null. @param array<string, Type> $schema */
    public static function normalizeRows(array $schema, array $rows): array
    {
        $out = [];
        foreach ($rows as $row) {
            $r = [];
            foreach ($schema as $name => $t) {
                $r[$name] = self::normalize($row[$name] ?? null, $t);
            }
            $out[] = $r;
        }
        return $out;
    }

    public static function normalize(mixed $v, Type $t): mixed
    {
        if ($v === null) {
            return null;
        }
        switch ($t->kind) {
            case 'struct':
                $o = [];
                foreach ($t->children as $i => $c) {
                    $o[$t->names[$i]] = self::normalize($v[$t->names[$i]] ?? null, $c);
                }
                return $o;
            case 'list':
                return \array_map(fn ($x) => self::normalize($x, $t->children[0]), $v);
            case 'map':
                $o = [];
                foreach ($v as $k => $x) {
                    if (\array_is_list($v) && \is_array($x) && \count($x) === 2 && !\in_array($t->children[1]->kind, ['list', 'struct', 'map'], true)) {
                        $o[] = [$x[0], self::normalize($x[1], $t->children[1])]; // (pairs)
                    } elseif (\array_is_list($v) && \is_array($x) && \count($x) === 2 && \array_is_list($x) && !\is_array($x[0])) {
                        $o[] = [$x[0], self::normalize($x[1], $t->children[1])];
                    } else {
                        $o[$k] = self::normalize($x, $t->children[1]);
                    }
                }
                return $o;
        }
        return $v;
    }

    /** @param array<string, Type> $schema @param list<array<string, mixed>> $rows @param array<string, mixed> $options */
    public static function write(array $schema, array $rows, array $options = []): string
    {
        $w = new Writer($schema, ...$options);
        $out = '';
        foreach ($rows as $row) {
            $out .= $w->encode($row);
        }
        return $out . $w->finish();
    }

    /** @return list<array<string, mixed>> */
    public static function read(string $bytes, ValueMode $mode = ValueMode::Logical): array
    {
        return \iterator_to_array(Reader::fromString($bytes, $mode)->rows(), false);
    }

    /** A strict comparison text: types, NaN and the sign of zero all count. */
    public static function text(mixed $rows): string
    {
        return \var_export($rows, true);
    }

    /** The first difference between two row lists, for a failure message. */
    public static function firstDifference(array $want, array $got): ?string
    {
        if (\count($want) !== \count($got)) {
            return 'row count ' . \count($got) . ' instead of ' . \count($want);
        }
        foreach ($want as $i => $row) {
            if (self::text($row) !== self::text($got[$i])) {
                foreach ($row as $k => $v) {
                    if (self::text($v) !== self::text($got[$i][$k] ?? null)) {
                        return "row $i column $k: wanted " . \substr(self::text($v), 0, 200) . ' got ' . \substr(self::text($got[$i][$k] ?? null), 0, 200);
                    }
                }
                return "row $i differs";
            }
        }
        return null;
    }
}
