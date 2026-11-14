<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

use Charm\Parquet\PhysicalType;

/** PLAIN encoding of values. */
final class Plain
{
    private const CHUNK = 8192;

    /** @param list<mixed> $vals */
    public static function encode(array $vals, PhysicalType $phys): string
    {
        $n = \count($vals);
        if ($n === 0) {
            return '';
        }
        switch ($phys) {
            case PhysicalType::Int32:
            case PhysicalType::Int64:
            case PhysicalType::Float:
            case PhysicalType::Double:
                $fmt = match ($phys) {
                    PhysicalType::Int32 => 'V*',
                    PhysicalType::Int64 => 'P*',
                    PhysicalType::Float => 'g*',
                    default => 'e*',
                };
                if ($n <= self::CHUNK) {
                    return \pack($fmt, ...$vals);
                }
                $out = '';
                for ($i = 0; $i < $n; $i += self::CHUNK) {
                    $out .= \pack($fmt, ...\array_slice($vals, $i, self::CHUNK));
                }
                return $out;
            case PhysicalType::Boolean:
                $out = '';
                for ($i = 0; $i < $n; $i += 8) {
                    $b = 0;
                    for ($k = 0; $k < 8 && $i + $k < $n; $k++) {
                        if ($vals[$i + $k]) {
                            $b |= 1 << $k;
                        }
                    }
                    $out .= \chr($b);
                }
                return $out;
            case PhysicalType::ByteArray:
                $out = '';
                foreach ($vals as $v) {
                    $out .= \pack('V', \strlen($v)) . $v;
                }
                return $out;
            default: // FIXED_LEN_BYTE_ARRAY
                return \implode('', $vals);
        }
    }
}
