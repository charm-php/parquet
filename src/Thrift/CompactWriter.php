<?php

declare(strict_types=1);

namespace Charm\Parquet\Thrift;

/**
 * Thrift's compact protocol, written to a string. A struct is a list of [fieldId, type, value]; a list or set value is
 * [elementType, items]; a map is [keyType, valueType, [[key, value], ...]]; a nested struct is a list of fields.
 */
final class CompactWriter
{
    public const BOOL = 1; // (true or false: the field type carries the value)
    public const BYTE = 3;
    public const I16 = 4;
    public const I32 = 5;
    public const I64 = 6;
    public const DOUBLE = 7;
    public const BINARY = 8;
    public const LIST = 9;
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

    /** @param list<array{int, int, mixed}> $fields ascending field ids */
    public static function struct(array $fields): string
    {
        $out = '';
        $last = 0;
        foreach ($fields as [$id, $type, $value]) {
            if ($value === null) {
                continue;
            }
            $isBool = $type === self::BOOL;
            $wire = $isBool ? ($value ? 1 : 2) : $type;
            $delta = $id - $last;
            $out .= $delta > 0 && $delta <= 15 ? \chr(($delta << 4) | $wire) : \chr($wire) . self::zigzag($id);
            $last = $id;
            if (!$isBool) {
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
                [$elemType, $items] = $value;
                $n = \count($items);
                $out = $n < 15 ? \chr(($n << 4) | $elemType) : \chr(0xF0 | $elemType) . self::varint($n);
                foreach ($items as $item) {
                    $out .= $elemType === self::STRUCT ? self::struct($item) : ($elemType === self::BOOL ? \chr($item ? 1 : 2) : self::value($elemType, $item));
                }
                return $out;
            case self::STRUCT:
                return self::struct($value);
        }
        throw new \InvalidArgumentException("Thrift type $type");
    }
}
