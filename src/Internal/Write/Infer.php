<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

use Charm\Parquet\{TimeUnit, Type};

/**
 * Type inference from sample rows (Type::infer()). A column's shape is merged over all the rows it appears in, as a small tree:
 * ['k' => 'null'|'bool'|'int'|'float'|'string'|'bytes'|'time'|'empty'|'list'|'struct', 'e' => element tree (list), 'f' => [name => tree] (struct)].
 */
final class Infer
{
    /**
     * @param iterable<mixed> $rows
     * @return array<string, Type>
     */
    public static function rows(iterable $rows): array
    {
        $fields = [];
        $n = 0;
        foreach ($rows as $row) {
            if (!\is_array($row)) {
                throw new \TypeError("row $n: expected an array, got " . \get_debug_type($row));
            }
            foreach ($row as $name => $v) {
                $name = (string) $name;
                $t = self::shape($v, $name, $n);
                $fields[$name] = isset($fields[$name]) ? self::merge($fields[$name], $t, $name, $n) : $t;
            }
            ++$n;
        }
        if ($fields === []) {
            throw new \ValueError('Cannot infer a schema from ' . ($n === 0 ? 'no rows' : 'rows without columns'));
        }
        $out = [];
        foreach ($fields as $name => $t) {
            $out[$name] = self::type($t);
        }
        return $out;
    }

    /** @return array<string, mixed> */
    private static function shape(mixed $v, string $path, int $row): array
    {
        if ($v === null) {
            return ['k' => 'null'];
        }
        if (\is_bool($v)) {
            return ['k' => 'bool'];
        }
        if (\is_int($v)) {
            return ['k' => 'int'];
        }
        if (\is_float($v)) {
            return ['k' => 'float'];
        }
        if (\is_string($v)) {
            return ['k' => \preg_match('//u', $v) === 1 ? 'string' : 'bytes'];
        }
        if ($v instanceof \DateTimeInterface) {
            return ['k' => 'time'];
        }
        if (\is_array($v)) {
            if ($v === []) {
                return ['k' => 'empty'];
            }
            if (\array_is_list($v)) {
                $e = null;
                foreach ($v as $x) {
                    $s = self::shape($x, $path . '[]', $row);
                    $e = $e === null ? $s : self::merge($e, $s, $path . '[]', $row);
                }
                return ['k' => 'list', 'e' => $e];
            }
            $f = [];
            foreach ($v as $key => $x) {
                if (\is_int($key)) {
                    throw new \ValueError("column '$path', row $row: an array with integer keys that is not a list (maps are not inferred; write a Type::map)");
                }
                $f[$key] = self::shape($x, $path . '.' . $key, $row);
            }
            return ['k' => 'struct', 'f' => $f];
        }
        throw new \TypeError("column '$path', row $row: cannot infer a type for " . \get_debug_type($v));
    }

    /**
     * @param array<string, mixed> $a
     * @param array<string, mixed> $b
     * @return array<string, mixed>
     */
    private static function merge(array $a, array $b, string $path, int $row): array
    {
        $ka = $a['k'];
        $kb = $b['k'];
        if ($ka === 'null') {
            return $b;
        }
        if ($kb === 'null') {
            return $a;
        }
        if ($ka === 'empty' && ($kb === 'list' || $kb === 'struct' || $kb === 'empty')) {
            return $b;
        }
        if ($kb === 'empty' && ($ka === 'list' || $ka === 'struct')) {
            return $a;
        }
        if ($ka === $kb) {
            if ($ka === 'list') {
                return ['k' => 'list', 'e' => self::merge($a['e'], $b['e'], $path . '[]', $row)];
            }
            if ($ka === 'struct') {
                $f = $a['f'];
                foreach ($b['f'] as $name => $t) {
                    $f[$name] = isset($f[$name]) ? self::merge($f[$name], $t, $path . '.' . $name, $row) : $t;
                }
                return ['k' => 'struct', 'f' => $f];
            }
            return $a;
        }
        if (($ka === 'int' && $kb === 'float') || ($ka === 'float' && $kb === 'int')) {
            return ['k' => 'float'];
        }
        if (($ka === 'string' && $kb === 'bytes') || ($ka === 'bytes' && $kb === 'string')) {
            return ['k' => 'bytes'];
        }
        throw new \ValueError("column '$path', row $row: cannot combine " . self::name($ka) . ' and ' . self::name($kb));
    }

    private static function name(string $k): string
    {
        return match ($k) {
            'bool' => 'bool', 'int' => 'int', 'float' => 'float', 'string' => 'string', 'bytes' => 'bytes (invalid UTF-8)',
            'time' => 'DateTimeInterface', 'empty' => 'an empty array', 'list' => 'a list', 'struct' => 'an array with string keys (a struct)',
            default => $k,
        };
    }

    /** @param array<string, mixed> $t */
    private static function type(array $t): Type
    {
        switch ($t['k']) {
            case 'bool':
                return Type::boolean();
            case 'int':
                return Type::int64();
            case 'float':
                return Type::double();
            case 'bytes':
                return Type::bytes();
            case 'time':
                return Type::timestamp(TimeUnit::Micros, utc: true);
            case 'list':
                return Type::list(self::type($t['e']));
            case 'struct':
                $f = [];
                foreach ($t['f'] as $name => $x) {
                    $f[$name] = self::type($x);
                }
                return Type::struct($f);
            case 'empty':
                return Type::list(Type::string());
            default: // 'string' and 'null' (a column that was never anything but null)
                return Type::string();
        }
    }
}
