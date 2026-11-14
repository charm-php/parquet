<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

use Charm\Parquet\TimeUnit;

/** Dates, times, timestamps and decimals from the forms the Writer accepts to the stored integers and bytes (exact integer arithmetic). */
final class Temporal
{
    /** @var array<string, int> */
    private static array $dayCache = [];

    /** A date: "YYYY-MM-DD" (a signed or longer year is accepted), a DateTimeInterface (its own calendar date), or the raw day count. */
    public static function date(mixed $v): int
    {
        if (\is_int($v)) {
            return $v >= -(1 << 31) && $v <= (1 << 31) - 1 ? $v : throw new \ValueError("$v days do not fit in 32 bits");
        }
        if ($v instanceof \DateTimeInterface) {
            $v = $v->format('Y-m-d');
        }
        if (!\is_string($v)) {
            throw new \TypeError('expected a date string, DateTimeInterface or int, got ' . \get_debug_type($v));
        }
        if (\preg_match('/^(-?\d{4,})-(\d{2})-(\d{2})$/', $v, $m) !== 1) {
            throw new \ValueError("'$v' is not a date (YYYY-MM-DD)");
        }
        $days = self::days($v, (int) $m[1], (int) $m[2], (int) $m[3]);
        return $days >= -(1 << 31) && $days <= (1 << 31) - 1 ? $days : throw new \ValueError("'$v' is outside the range of a 32-bit day count");
    }

    /** Days since 1970-01-01 of a proleptic Gregorian date; month and day are checked. */
    private static function days(string $text, int $y, int $m, int $d): int
    {
        $leap = ($y % 4 === 0 && $y % 100 !== 0) || $y % 400 === 0;
        $dim = [31, $leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        if ($m < 1 || $m > 12 || $d < 1 || $d > $dim[$m - 1]) {
            throw new \ValueError("'$text' is not a calendar date");
        }
        if ($m <= 2) {
            --$y;
        }
        $era = \intdiv($y >= 0 ? $y : $y - 399, 400);
        $yoe = $y - $era * 400;
        $doy = \intdiv(153 * ($m + ($m > 2 ? -3 : 9)) + 2, 5) + $d - 1;
        $doe = $yoe * 365 + \intdiv($yoe, 4) - \intdiv($yoe, 100) + $doy;
        return $era * 146097 + $doe - 719468;
    }

    private static function perSecond(TimeUnit $u): int
    {
        return match ($u) {
            TimeUnit::Millis => 1000,
            TimeUnit::Micros => 1000000,
            TimeUnit::Nanos => 1000000000,
        };
    }

    /** The fraction digits "123" (of a second) as a count of $per-ths of a second; digits beyond the unit must be zeros. */
    private static function fraction(string $text, string $digits, int $per): int
    {
        $width = $per === 1000000 ? 6 : ($per === 1000 ? 3 : 9);
        if (\strlen($digits) === $width) {
            return (int) $digits;
        }
        if (\strlen($digits) > $width) {
            if (\ltrim(\substr($digits, $width), '0') !== '') {
                throw new \ValueError("'$text' has more fractional digits than the unit holds");
            }
            $digits = \substr($digits, 0, $width);
        }
        return (int) \str_pad($digits, $width, '0');
    }

    /** A time of day: "HH:MM:SS[.fff]" (with a leading '-' and hours above 23 as the Reader writes them) or the raw count of units. */
    public static function time(mixed $v, TimeUnit $unit, int $min, int $max): int
    {
        if (\is_int($v)) {
            return $v >= $min && $v <= $max ? $v : throw new \ValueError("$v is outside $min .. $max");
        }
        if (!\is_string($v)) {
            throw new \TypeError('expected a time string or int, got ' . \get_debug_type($v));
        }
        if (\preg_match('/^(-?)(\d+):(\d{2}):(\d{2})(?:\.(\d{1,9}))?$/', $v, $m) !== 1 || (int) $m[3] > 59 || (int) $m[4] > 59) {
            throw new \ValueError("'$v' is not a time (HH:MM:SS[.fraction])");
        }
        $per = self::perSecond($unit);
        $x = ((int) $m[2] * 3600 + (int) $m[3] * 60 + (int) $m[4]) * $per + self::fraction($v, $m[5] ?? '', $per);
        if (!\is_int($x) || \strlen($m[2]) > 12) {
            throw new \ValueError("'$v' does not fit");
        }
        $x = $m[1] === '-' ? -$x : $x;
        return $x >= $min && $x <= $max ? $x : throw new \ValueError("'$v' is outside the range of the type");
    }

    /** A timestamp: "YYYY-MM-DDTHH:MM:SS[.fff][Z]" (Z only for a UTC type), a DateTimeInterface (its instant), or the raw count of units. */
    public static function timestamp(mixed $v, TimeUnit $unit, bool $utc): int
    {
        $per = self::perSecond($unit);
        if (\is_int($v)) {
            return $v;
        }
        if ($v instanceof \DateTimeInterface) {
            $x = $v->getTimestamp() * $per + \intdiv((int) $v->format('u') * $per, 1000000);
            return \is_int($x) ? $x : throw new \ValueError('the instant does not fit in 64 bits');
        }
        if (!\is_string($v)) {
            throw new \TypeError('expected a timestamp string, DateTimeInterface or int, got ' . \get_debug_type($v));
        }
        if (\preg_match('/^(-?\d{4,})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z?)$/', $v, $m) !== 1 || (int) $m[4] > 23 || (int) $m[5] > 59 || (int) $m[6] > 59) {
            throw new \ValueError("'$v' is not a timestamp (YYYY-MM-DDTHH:MM:SS[.fraction][Z])");
        }
        if ($m[8] === 'Z' && !$utc) {
            throw new \ValueError("'$v' has a Z suffix but the type is not adjusted to UTC");
        }
        // (the same few dates come over and over: the day count is cached by the text of the date)
        $key = \substr($v, 0, \strlen($m[1]) + 6);
        $days = self::$dayCache[$key] ?? null;
        if ($days === null) {
            if (\count(self::$dayCache) >= 4096) {
                self::$dayCache = [];
            }
            $days = self::$dayCache[$key] = self::days($v, (int) $m[1], (int) $m[2], (int) $m[3]);
        }
        $secs = $days * 86400 + (int) $m[4] * 3600 + (int) $m[5] * 60 + (int) $m[6];
        return \is_int($secs) ? self::scaled($v, $secs, $per, self::fraction($v, $m[7] ?? '', $per)) : throw new \ValueError("'$v' does not fit in 64 bits");
    }

    /** $secs * $per + $frac (0 <= $frac < $per) without ever leaving 64-bit ints. */
    private static function scaled(string $text, int $secs, int $per, int $frac): int
    {
        if ($secs >= 0) {
            return $secs <= \intdiv(\PHP_INT_MAX - $frac, $per) ? $secs * $per + $frac : throw new \ValueError("'$text' does not fit in 64 bits");
        }
        $m = $secs + 1; // x = m * per - (per - frac), with m <= 0
        if ($m < \intdiv(\PHP_INT_MIN, $per)) {
            throw new \ValueError("'$text' does not fit in 64 bits");
        }
        $y = $m * $per;
        return $y >= \PHP_INT_MIN + ($per - $frac) ? $y - ($per - $frac) : throw new \ValueError("'$text' does not fit in 64 bits");
    }

    /**
     * The digits of a non-negative integer (no leading zeros) as big-endian bytes, $len of them, two's complement when $neg.
     * With $signedCheck the magnitude must fit $len bytes as a signed number; without, as an unsigned one.
     */
    public static function decimalToBytes(string $digits, bool $neg, int $len, bool $signedCheck = true): string
    {
        $limbs = [0];
        for ($i = 0, $n = \strlen($digits); $i < $n; $i += 9) {
            $chunk = \substr($digits, $i, 9);
            $mult = 10 ** \strlen($chunk);
            $carry = (int) $chunk;
            foreach ($limbs as $k => $l) {
                $t = $l * $mult + $carry;
                $limbs[$k] = $t & 0xFFFFFFFF;
                $carry = $t >> 32;
            }
            if ($carry > 0) {
                $limbs[] = $carry;
            }
        }
        $bytes = \strrev(\pack('V*', ...$limbs));
        $bytes = \ltrim($bytes, "\x00");
        if (\strlen($bytes) > $len || ($signedCheck && \strlen($bytes) === $len && (\ord($bytes[0]) & 0x80) !== 0 && !($neg && $bytes === "\x80" . \str_repeat("\x00", $len - 1)))) {
            throw new \ValueError("$digits does not fit in $len bytes");
        }
        $bytes = \str_pad($bytes, $len, "\x00", \STR_PAD_LEFT);
        if ($neg) {
            $carry = 1;
            for ($i = $len - 1; $i >= 0; $i--) {
                $x = ((~\ord($bytes[$i])) & 0xFF) + $carry;
                $bytes[$i] = \chr($x & 0xFF);
                $carry = $x >> 8;
            }
        }
        return $bytes;
    }
}
