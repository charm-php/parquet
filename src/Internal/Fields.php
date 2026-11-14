<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\FormatException;

/** Typed access to the fields of a parsed Thrift struct (array<int, mixed>): a field of the wrong type is a FormatException. */
final class Fields
{
    /** @param array<int, mixed> $s */
    public static function int(array $s, int $id, string $what): int
    {
        $v = $s[$id] ?? throw new FormatException("$what is missing");
        return \is_int($v) ? $v : throw new FormatException("$what is not an integer");
    }

    /** @param array<int, mixed> $s */
    public static function optInt(array $s, int $id, string $what): ?int
    {
        if (!isset($s[$id])) {
            return null;
        }
        return \is_int($s[$id]) ? $s[$id] : throw new FormatException("$what is not an integer");
    }

    /** An integer field that is purely informational: a value of another type (some writers put something else there) counts as absent. @param array<int, mixed> $s */
    public static function lenientInt(array $s, int $id): ?int
    {
        return isset($s[$id]) && \is_int($s[$id]) ? $s[$id] : null;
    }

    /** @param array<int, mixed> $s */
    public static function optBool(array $s, int $id, string $what): ?bool
    {
        if (!isset($s[$id])) {
            return null;
        }
        return \is_bool($s[$id]) ? $s[$id] : throw new FormatException("$what is not a boolean");
    }

    /** @param array<int, mixed> $s */
    public static function optString(array $s, int $id, string $what): ?string
    {
        if (!isset($s[$id])) {
            return null;
        }
        return \is_string($s[$id]) ? $s[$id] : throw new FormatException("$what is not a string");
    }

    /**
     * @param array<int, mixed> $s
     * @return array<int, mixed>
     */
    public static function struct(array $s, int $id, string $what): array
    {
        $v = $s[$id] ?? throw new FormatException("$what is missing");
        return \is_array($v) ? $v : throw new FormatException("$what is not a struct");
    }

    /**
     * @param array<int, mixed> $s
     * @return array<int, mixed>|null
     */
    public static function optStruct(array $s, int $id, string $what): ?array
    {
        if (!isset($s[$id])) {
            return null;
        }
        return \is_array($s[$id]) ? $s[$id] : throw new FormatException("$what is not a struct");
    }

    /**
     * @param array<int, mixed> $s
     * @return list<mixed>
     */
    public static function list(array $s, int $id, string $what): array
    {
        $v = $s[$id] ?? throw new FormatException("$what is missing");
        return \is_array($v) && \array_is_list($v) ? $v : throw new FormatException("$what is not a list");
    }

    /**
     * @param array<int, mixed> $s
     * @return list<mixed>
     */
    public static function optList(array $s, int $id, string $what): array
    {
        if (!isset($s[$id])) {
            return [];
        }
        return \is_array($s[$id]) && \array_is_list($s[$id]) ? $s[$id] : throw new FormatException("$what is not a list");
    }
}
