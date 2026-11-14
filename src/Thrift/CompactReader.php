<?php

declare(strict_types=1);

namespace Charm\Parquet\Thrift;

use Charm\Parquet\FormatException;

/**
 * Thrift's compact protocol, read from a string, with every read bounds-checked. A struct comes back as `array<int, mixed>`
 * keyed by field id (so fields this package does not know are parsed and carried along, which is also how they are skipped).
 * Lists and sets are PHP lists; a map is a list of [key, value] pairs; i16, i32 and i64 are ints; binary and string are strings.
 *
 * Limits: nesting depth {@see self::MAX_DEPTH}; a container cannot announce more elements than there are bytes left (every
 * element takes at least one byte), so memory use is bounded by the input size.
 */
final class CompactReader
{
    public const MAX_DEPTH = 32;

    private int $end;
    private int $depth = 0;

    public function __construct(private readonly string $buf, private int $pos = 0, ?int $end = null)
    {
        $this->end = $end ?? \strlen($buf);
        if ($this->pos < 0 || $this->end > \strlen($buf) || $this->pos > $this->end) {
            throw new FormatException('Thrift reader started outside its buffer');
        }
    }

    public function position(): int
    {
        return $this->pos;
    }

    /** @return array<int, mixed> */
    public function readStruct(): array
    {
        if (++$this->depth > self::MAX_DEPTH) {
            throw new FormatException('Thrift data nested deeper than ' . self::MAX_DEPTH . ' levels');
        }
        $out = [];
        $last = 0;
        while (true) {
            $b = $this->byte();
            if ($b === 0) {
                break;
            }
            $type = $b & 0x0F;
            $delta = $b >> 4;
            $id = $delta !== 0 ? $last + $delta : $this->zigzag();
            if ($id < 0 || $id > 0x7FFF) { // (field ids are 16-bit; a larger one would also overflow the next delta into a float)
                throw new FormatException("Thrift field id $id is out of range");
            }
            $last = $id;
            $out[$id] = $type === 1 ? true : ($type === 2 ? false : $this->value($type));
        }
        --$this->depth;
        return $out;
    }

    private function byte(): int
    {
        if ($this->pos >= $this->end) {
            throw new FormatException('Thrift data ends unexpectedly');
        }
        return \ord($this->buf[$this->pos++]);
    }

    private function varint(): int
    {
        $v = 0;
        for ($shift = 0; $shift < 70; $shift += 7) {
            $b = $this->byte();
            $v |= ($b & 0x7F) << $shift;
            if (($b & 0x80) === 0) {
                return $v;
            }
        }
        throw new FormatException('Thrift varint longer than 10 bytes');
    }

    private function zigzag(): int
    {
        $v = $this->varint();
        return (($v >> 1) & \PHP_INT_MAX) ^ -($v & 1);
    }

    /** A length or element count that must fit in what is left of the buffer. */
    private function count(): int
    {
        $n = $this->varint();
        if ($n < 0 || $n > $this->end - $this->pos) {
            throw new FormatException('Thrift length ' . $n . ' exceeds the data left');
        }
        return $n;
    }

    private function value(int $type): mixed
    {
        switch ($type) {
            case 1:
                return true;
            case 2:
                return false;
            case 3:
                $v = $this->byte();
                return $v > 127 ? $v - 256 : $v;
            case 4:
            case 5:
            case 6:
                return $this->zigzag();
            case 7:
                if ($this->end - $this->pos < 8) {
                    throw new FormatException('Thrift data ends unexpectedly');
                }
                /** @var array{1: float} $u */
                $u = \unpack('e', $this->buf, $this->pos);
                $this->pos += 8;
                return $u[1];
            case 8:
                $n = $this->count();
                $s = \substr($this->buf, $this->pos, $n);
                $this->pos += $n;
                return $s;
            case 9:
            case 10:
                $h = $this->byte();
                $n = $h >> 4;
                $et = $h & 0x0F;
                if ($n === 15) {
                    $n = $this->count();
                } elseif ($n > $this->end - $this->pos) {
                    throw new FormatException('Thrift list longer than the data left');
                }
                if ($et === 0 || $et > 12) {
                    throw new FormatException("Thrift list with element type $et");
                }
                if (++$this->depth > self::MAX_DEPTH) {
                    throw new FormatException('Thrift data nested deeper than ' . self::MAX_DEPTH . ' levels');
                }
                $v = [];
                for ($i = 0; $i < $n; $i++) {
                    $v[] = $et === 1 || $et === 2 ? $this->byte() === 1 : $this->value($et);
                }
                --$this->depth;
                return $v;
            case 11:
                $n = $this->count();
                $v = [];
                if ($n === 0) {
                    return $v;
                }
                $kt = $this->byte();
                $ktype = $kt >> 4;
                $vtype = $kt & 0x0F;
                if ($ktype === 0 || $ktype > 12 || $vtype === 0 || $vtype > 12) {
                    throw new FormatException('Thrift map with an invalid key or value type');
                }
                if (++$this->depth > self::MAX_DEPTH) {
                    throw new FormatException('Thrift data nested deeper than ' . self::MAX_DEPTH . ' levels');
                }
                for ($i = 0; $i < $n; $i++) {
                    $k = $ktype === 1 || $ktype === 2 ? $this->byte() === 1 : $this->value($ktype);
                    $v[] = [$k, $vtype === 1 || $vtype === 2 ? $this->byte() === 1 : $this->value($vtype)];
                }
                --$this->depth;
                return $v;
            case 12:
                return $this->readStruct();
        }
        throw new FormatException("Thrift compact type $type is not valid");
    }
}
