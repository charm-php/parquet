<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

/** A minimal Thrift compact protocol writer, for building files and structs by hand in tests. A struct is a list of [fieldId, type, value]. */
final class Thrift
{
    public const BOOL_TRUE = 1;
    public const BOOL_FALSE = 2;
    public const BYTE = 3;
    public const I16 = 4;
    public const I32 = 5;
    public const I64 = 6;
    public const DOUBLE = 7;
    public const BINARY = 8;
    public const LIST = 9;
    public const SET = 10;
    public const MAP = 11;
    public const STRUCT = 12;

    public static function varint(int $v): string
    {
        $out = '';
        do {
            $b = $v & 0x7F;
            $v = ($v >> 7) & (\PHP_INT_MAX >> 6);
            $out .= \chr($v !== 0 ? $b | 0x80 : $b);
        } while ($v !== 0);
        return $out;
    }

    public static function zigzag(int $v): string
    {
        return self::varint(($v << 1) ^ ($v >> 63));
    }

    /** @param list<array{int, int, mixed}> $fields */
    public static function struct(array $fields): string
    {
        $out = '';
        $last = 0;
        foreach ($fields as [$id, $type, $value]) {
            $wire = $type;
            if ($type === self::BOOL_TRUE || $type === self::BOOL_FALSE) {
                $wire = $value ? self::BOOL_TRUE : self::BOOL_FALSE;
            }
            $delta = $id - $last;
            if ($delta > 0 && $delta <= 15) {
                $out .= \chr(($delta << 4) | $wire);
            } else {
                $out .= \chr($wire) . self::zigzag($id);
            }
            $last = $id;
            if ($type !== self::BOOL_TRUE && $type !== self::BOOL_FALSE) {
                $out .= self::value($type, $value);
            }
        }
        return $out . "\x00";
    }

    public static function value(int $type, mixed $value): string
    {
        switch ($type) {
            case self::BYTE:
                return \chr($value & 0xFF);
            case self::I16:
            case self::I32:
            case self::I64:
                return self::zigzag($value);
            case self::DOUBLE:
                return \pack('e', $value);
            case self::BINARY:
                return self::varint(\strlen($value)) . $value;
            case self::LIST:
            case self::SET:
                [$elemType, $items] = $value;
                $n = \count($items);
                $out = $n < 15 ? \chr(($n << 4) | $elemType) : \chr(0xF0 | $elemType) . self::varint($n);
                foreach ($items as $item) {
                    $out .= $elemType === self::STRUCT ? self::struct($item) : ($elemType === self::BOOL_TRUE ? \chr($item ? 1 : 2) : self::value($elemType, $item));
                }
                return $out;
            case self::MAP:
                [$kt, $vt, $pairs] = $value;
                if ($pairs === []) {
                    return "\x00";
                }
                $out = self::varint(\count($pairs)) . \chr(($kt << 4) | $vt);
                foreach ($pairs as [$k, $v]) {
                    $out .= self::value($kt, $k) . self::value($vt, $v);
                }
                return $out;
            case self::STRUCT:
                return self::struct($value);
        }
        throw new \InvalidArgumentException("type $type");
    }
}
