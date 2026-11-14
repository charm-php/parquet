<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{TimeUnit, Type};
use Charm\Parquet\Internal\Logical;

/**
 * Random schemas and rows for the writer tests, from a seed (mt_rand): every type, nesting up to depth 4, required and optional at every
 * level, and edge values. Values are in the form Reader::rows() returns in logical mode, so a file written from them must read back identical.
 */
final class RandomData
{
    private const LEAVES = ['bool', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 'float', 'double', 'float16', 'string', 'bytes',
        'fixed', 'decimal', 'date', 'time', 'timestamp', 'uuid', 'json'];

    public function __construct(private readonly bool $extremes = true)
    {
    }

    /** @return array<string, Type> */
    public function schema(int $seed, int $maxDepth = 4, int $columns = 0): array
    {
        \mt_srand($seed);
        $n = $columns > 0 ? $columns : \mt_rand(1, 6);
        $s = [];
        for ($i = 0; $i < $n; $i++) {
            $s['c' . $i] = $this->type($maxDepth);
        }
        return $s;
    }

    public function type(int $depth): Type
    {
        $r = \mt_rand(0, 9);
        $t = match (true) {
            $depth > 0 && $r === 0 => Type::list($this->type($depth - 1)),
            $depth > 0 && $r === 1 => Type::struct($this->fields($depth - 1)),
            $depth > 0 && $r === 2 => Type::map($this->leaf(), $this->type($depth - 1)),
            default => $this->leaf(),
        };
        return \mt_rand(0, 3) === 0 ? $t->required() : $t;
    }

    /** @return array<string, Type> */
    private function fields(int $depth): array
    {
        $f = [];
        for ($i = \mt_rand(1, 3); $i > 0; $i--) {
            $f['f' . \count($f)] = $this->type($depth);
        }
        return $f;
    }

    private function leaf(): Type
    {
        $k = self::LEAVES[\mt_rand(0, \count(self::LEAVES) - 1)];
        $unit = [TimeUnit::Millis, TimeUnit::Micros, TimeUnit::Nanos][\mt_rand(0, 2)];
        return match ($k) {
            'bool' => Type::boolean(), 'int8' => Type::int8(), 'int16' => Type::int16(), 'int32' => Type::int32(), 'int64' => Type::int64(),
            'uint8' => Type::uint8(), 'uint16' => Type::uint16(), 'uint32' => Type::uint32(), 'uint64' => Type::uint64(),
            'float' => Type::float(), 'double' => Type::double(), 'float16' => Type::float16(), 'string' => Type::string(), 'bytes' => Type::bytes(),
            'fixed' => Type::fixed(\mt_rand(1, 20)),
            'decimal' => ($p = [1, 2, 4, 9, 10, 18, 19, 24, 38][\mt_rand(0, 8)]) ? Type::decimal($p, \mt_rand(0, $p)) : Type::decimal(5, 2),
            'date' => Type::date(),
            'time' => Type::time($unit === TimeUnit::Nanos && false ? TimeUnit::Micros : $unit, \mt_rand(0, 1) === 1),
            'timestamp' => Type::timestamp($unit, \mt_rand(0, 1) === 1),
            'uuid' => Type::uuid(), 'json' => Type::json(),
        };
    }

    /** @param array<string, Type> $schema @return list<array<string, mixed>> */
    public function rows(array $schema, int $n, int $seed): array
    {
        \mt_srand($seed);
        $rows = [];
        for ($i = 0; $i < $n; $i++) {
            $row = [];
            foreach ($schema as $name => $t) {
                $v = $this->value($t);
                if ($v !== null || \mt_rand(0, 1) === 0) {
                    $row[$name] = $v; // (a null is sometimes a missing key)
                }
            }
            $rows[] = $row;
        }
        return $rows;
    }

    public function value(Type $t): mixed
    {
        if ($t->nullable && \mt_rand(0, 5) === 0) {
            return null;
        }
        $p = $t->params;
        switch ($t->kind) {
            case 'bool':
                return \mt_rand(0, 1) === 1;
            case 'int8':
                return $this->int(-128, 127);
            case 'int16':
                return $this->int(-32768, 32767);
            case 'int32':
                return $this->int(-2147483648, 2147483647);
            case 'int64':
                return \mt_rand(0, 4) === 0 ? [\PHP_INT_MIN, \PHP_INT_MAX, 0, -1, 1][\mt_rand(0, 4)] : (\mt_rand() << 31 ^ \mt_rand()) * (\mt_rand(0, 1) ? 1 : -1);
            case 'uint8':
                return $this->int(0, 255);
            case 'uint16':
                return $this->int(0, 65535);
            case 'uint32':
                return $this->int(0, 4294967295);
            case 'uint64':
                $r = \mt_rand(0, 5);
                return match (true) {
                    $r === 0 => '18446744073709551615',
                    $r === 1 => '9223372036854775808',
                    $r === 2 => 0,
                    $r === 3 => \PHP_INT_MAX,
                    default => \mt_rand(0, \PHP_INT_MAX >> 8),
                };
            case 'float':
                return $this->float(true);
            case 'double':
                return $this->float(false);
            case 'float16':
                $h = \mt_rand(0, 0xFFFF);
                if ((($h >> 10) & 0x1F) === 0x1F && ($h & 0x3FF) !== 0) {
                    return \NAN; // (one NaN: a payload is not kept)
                }
                $exp = ($h >> 10) & 0x1F;
                $man = $h & 0x3FF;
                $sign = ($h & 0x8000) !== 0 ? -1.0 : 1.0;
                return $exp === 0 ? $sign * $man * 5.9604644775390625e-8 : ($exp === 31 ? $sign * \INF : $sign * (1 + $man / 1024) * (2.0 ** ($exp - 15)));
            case 'string':
                return $this->string();
            case 'json':
                return \mt_rand(0, 1) === 0 ? '{"a":' . \mt_rand(0, 99) . '}' : '[]';
            case 'bytes':
                return $this->bytes(\mt_rand(0, 3) === 0 ? 0 : \mt_rand(0, 12));
            case 'fixed':
                return $this->bytes($p['length']);
            case 'uuid':
                return \implode('-', [\bin2hex($this->bytes(4)), \bin2hex($this->bytes(2)), \bin2hex($this->bytes(2)), \bin2hex($this->bytes(2)), \bin2hex($this->bytes(6))]);
            case 'decimal':
                return $this->decimal($p['precision'], $p['scale']);
            case 'date':
                $r = \mt_rand(0, 9);
                $days = $r === 0 && $this->extremes ? [-719528, -719529, 2932896, 2932897, -2147483648, 2147483647, 0, 59, 60][\mt_rand(0, 8)] : \mt_rand(-700000, 2900000);
                return Logical::dateString($days);
            case 'time':
                [$per, $digits] = $this->unit($p['unit']);
                $x = \mt_rand(0, 4) === 0 ? [0, 1, $per - 1, 86400 * $per - 1][\mt_rand(0, 3)] : \mt_rand(0, 86400 * $per - 1);
                return Logical::timeString($x, $per, $digits);
            case 'timestamp':
                [$per, $digits] = $this->unit($p['unit']);
                $x = $this->timestampValue($per);
                $perDay = 86400 * $per;
                $days = \intdiv($x, $perDay);
                $rem = $x - $days * $perDay;
                if ($rem < 0) {
                    $rem += $perDay;
                    --$days;
                }
                return Logical::dateString($days) . 'T' . Logical::timeString($rem, $per, $digits) . ($p['utc'] ? 'Z' : '');
            case 'list':
                $r = \mt_rand(0, 5);
                $n = $r === 0 ? 0 : \mt_rand(1, 4);
                $out = [];
                for ($i = 0; $i < $n; $i++) {
                    $out[] = $this->value($t->children[0]);
                }
                return $out;
            case 'struct':
                $out = [];
                foreach ($t->children as $i => $c) {
                    $v = $this->value($c);
                    if ($v !== null || \mt_rand(0, 1) === 0) {
                        $out[$t->names[$i]] = $v;
                    }
                }
                return $out;
            case 'map':
                return $this->map($t);
        }
        throw new \LogicException($t->kind);
    }

    private function int(int $lo, int $hi): int
    {
        if (\mt_rand(0, 4) === 0) {
            $e = \array_values(\array_filter([$lo, $hi, 0, -1, 1], static fn (int $x): bool => $x >= $lo && $x <= $hi));
            return $e[\mt_rand(0, \count($e) - 1)];
        }
        return \mt_rand($lo, $hi);
    }

    private function float(bool $single): float
    {
        $r = \mt_rand(0, 9);
        $v = match (true) {
            $r === 0 => \NAN, $r === 1 => \INF, $r === 2 => -\INF, $r === 3 => 0.0, $r === 4 => -0.0,
            $r === 5 => 1.0e300 * (\mt_rand(0, 1) ? 1 : -1), $r === 6 => 5e-324,
            default => (\mt_rand() / \mt_getrandmax() - 0.5) * 10 ** \mt_rand(-5, 8),
        };
        return $single ? \unpack('g', \pack('g', $v))[1] : $v;
    }

    private function string(): string
    {
        $r = \mt_rand(0, 7);
        return match (true) {
            $r === 0 => '', $r === 1 => "a\x00b", $r === 2 => "\u{e9}\u{65e5}\u{672c}\u{1F600}", $r === 3 => \str_repeat('x', \mt_rand(0, 300)),
            $r === 4 => (string) \mt_rand(0, 9999), // (numeric text: PHP turns it into an int array key)
            default => 's' . \mt_rand(0, 40),
        };
    }

    private function bytes(int $n): string
    {
        $s = '';
        for ($i = 0; $i < $n; $i++) {
            $s .= \chr(\mt_rand(0, 255));
        }
        return $s;
    }

    private function decimal(int $p, int $s): string
    {
        $len = \mt_rand(0, 3) === 0 ? $p : \mt_rand(1, $p);
        $digits = '';
        for ($i = 0; $i < $len; $i++) {
            $digits .= \mt_rand(0, 3) === 0 ? '9' : (string) \mt_rand(0, 9);
        }
        $digits = \ltrim($digits, '0');
        $neg = $digits !== '' && \mt_rand(0, 1) === 1;
        $digits = \str_pad($digits, $s + 1, '0', \STR_PAD_LEFT);
        $text = $s === 0 ? $digits : \substr($digits, 0, -$s) . '.' . \substr($digits, -$s);
        return ($neg ? '-' : '') . $text;
    }

    /** @return array{int, int} */
    private function unit(TimeUnit $u): array
    {
        return match ($u) {
            TimeUnit::Millis => [1000, 3],
            TimeUnit::Micros => [1000000, 6],
            TimeUnit::Nanos => [1000000000, 9],
        };
    }

    private function timestampValue(int $per): int
    {
        $r = \mt_rand(0, 9);
        if ($r === 0 && $this->extremes) {
            return [0, -1, 1, \PHP_INT_MAX, \PHP_INT_MIN][\mt_rand(0, 4)];
        }
        $max = (int) (\min(\PHP_INT_MAX, 253402300799 * $per) / 2);
        return \mt_rand(-$max, $max);
    }

    private function map(Type $t): array
    {
        $keyType = $t->children[0];
        $assoc = \in_array($keyType->kind, ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 'string', 'json', 'bytes', 'fixed', 'uuid', 'decimal', 'date', 'time', 'timestamp'], true);
        $n = \mt_rand(0, 3) === 0 ? 0 : \mt_rand(1, 4);
        $out = [];
        $seen = [];
        for ($i = 0; $i < $n; $i++) {
            $k = $this->keyValue($keyType);
            $id = $assoc ? (string) (\is_int($k) ? $k : $k) : \var_export($k, true);
            if (isset($seen[$id])) {
                continue;
            }
            $seen[$id] = true;
            $v = $this->value($t->children[1]);
            if ($assoc) {
                $out[$k] = $v;
            } else {
                $out[] = [$k, $v];
            }
        }
        if ($assoc && \count($out) !== \count($seen)) {
            return []; // (two keys became one: PHP normalised them; keep the generator simple)
        }
        return $out;
    }

    private function keyValue(Type $t): mixed
    {
        $v = $this->value($t->required());
        return $v;
    }
}
