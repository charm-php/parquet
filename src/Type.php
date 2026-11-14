<?php

declare(strict_types=1);

namespace Charm\Parquet;

/**
 * A column type for writing, built with the static constructors and made required with required() (the default is nullable, as in
 * pyarrow):
 *
 *     new Writer(['id' => Type::int64()->required(), 'name' => Type::string(), 'tags' => Type::list(Type::string()),
 *                 'price' => Type::decimal(18, 2), 'at' => Type::timestamp(TimeUnit::Micros, utc: true)]);
 *
 * Immutable; a Type says what a value is, the Writer decides how it is stored (see the README for the physical types and annotations).
 */
final class Type
{
    /**
     * @param array<string, mixed> $params
     * @param list<Type> $children list: [element]; map: [key, value]; struct: the fields (named by $names)
     * @param list<string> $names struct field names
     */
    private function __construct(
        public readonly string $kind,
        public readonly bool $nullable = true,
        public readonly array $params = [],
        public readonly array $children = [],
        public readonly array $names = [],
    ) {
    }

    /**
     * The schema of a sample of rows, ready for `new Writer(...)`: a column per key (in the order first seen), every column nullable.
     *
     * Rows are arrays [name => value]. PHP types map to: bool to boolean; int to int64; float to double; string to string when it is valid
     * UTF-8 (a column that has an invalid string in it becomes bytes); `\DateTimeInterface` to timestamp(micros, utc: true); a list to a list
     * of the element type merged over all elements and rows; an array with string keys to a struct whose fields are merged over all
     * rows (a field missing from some rows is simply nullable). int and float merge to double; null merges with anything. A column that
     * is only ever null becomes a nullable string, and one that is only ever an empty array a list of strings (nothing in the sample says
     * more). Anything else that conflicts (int and string, a list and a struct, ...) is a `\ValueError` naming the column path and the
     * row where it was seen, and an object other than a DateTimeInterface is a `\TypeError`.
     *
     * Never inferred: decimals (a float is a double, a numeric string a string), dates and times from strings, maps (an array with
     * integer keys that is not a list is a `\ValueError`: say `Type::map()` yourself), narrower integers, uuid, json, fixed-length
     * bytes. Pass your own Type for those columns next to the inferred ones.
     *
     * @param iterable<array<string, mixed>> $rows
     * @return array<string, Type>
     */
    public static function infer(iterable $rows): array
    {
        return Internal\Write\Infer::rows($rows);
    }

    public function required(): self
    {
        return new self($this->kind, false, $this->params, $this->children, $this->names);
    }

    public function nullable(): self
    {
        return new self($this->kind, true, $this->params, $this->children, $this->names);
    }

    public static function boolean(): self
    {
        return new self('bool');
    }

    public static function int8(): self
    {
        return new self('int8');
    }

    public static function int16(): self
    {
        return new self('int16');
    }

    public static function int32(): self
    {
        return new self('int32');
    }

    public static function int64(): self
    {
        return new self('int64');
    }

    public static function uint8(): self
    {
        return new self('uint8');
    }

    public static function uint16(): self
    {
        return new self('uint16');
    }

    public static function uint32(): self
    {
        return new self('uint32');
    }

    public static function uint64(): self
    {
        return new self('uint64');
    }

    public static function float(): self
    {
        return new self('float');
    }

    public static function double(): self
    {
        return new self('double');
    }

    /** An IEEE half precision float (stored in 2 bytes). */
    public static function float16(): self
    {
        return new self('float16');
    }

    /** UTF-8 text (the Writer refuses invalid UTF-8). */
    public static function string(): self
    {
        return new self('string');
    }

    public static function bytes(): self
    {
        return new self('bytes');
    }

    public static function fixed(int $length): self
    {
        if ($length < 1) {
            throw new \ValueError('A fixed-length type needs a length of at least 1');
        }
        return new self('fixed', true, ['length' => $length]);
    }

    /** An exact decimal of up to $precision digits with $scale of them after the point (stored as INT32, INT64 or fixed-length bytes by precision). */
    public static function decimal(int $precision, int $scale = 0): self
    {
        if ($precision < 1 || $precision > 38 || $scale < 0 || $scale > $precision) {
            throw new \ValueError('A decimal needs 1 <= precision <= 38 and 0 <= scale <= precision');
        }
        return new self('decimal', true, ['precision' => $precision, 'scale' => $scale]);
    }

    public static function date(): self
    {
        return new self('date');
    }

    public static function time(TimeUnit $unit, bool $utc = false): self
    {
        return new self('time', true, ['unit' => $unit, 'utc' => $utc]);
    }

    public static function timestamp(TimeUnit $unit, bool $utc = false): self
    {
        return new self('timestamp', true, ['unit' => $unit, 'utc' => $utc]);
    }

    public static function uuid(): self
    {
        return new self('uuid');
    }

    /** Text that is JSON (not parsed or validated beyond UTF-8). */
    public static function json(): self
    {
        return new self('json');
    }

    public static function list(self $element): self
    {
        return new self('list', true, [], [$element]);
    }

    /** @param array<string, Type> $fields */
    public static function struct(array $fields): self
    {
        if ($fields === []) {
            throw new \ValueError('A struct needs at least one field');
        }
        foreach ($fields as $name => $t) {
            if (!$t instanceof self || $name === '') {
                throw new \ValueError('Struct fields are named Types');
            }
        }
        return new self('struct', true, [], \array_values($fields), \array_map('strval', \array_keys($fields)));
    }

    /** A map; its keys are always required (a Parquet rule). */
    public static function map(self $key, self $value): self
    {
        return new self('map', true, [], [$key->required(), $value]);
    }
}
