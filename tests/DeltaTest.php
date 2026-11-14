<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{ParquetException, PhysicalType, Reader};
use Charm\Parquet\Internal\{ByteStreamSplit, Delta};
use PHPUnit\Framework\TestCase;

/** DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY and BYTE_STREAM_SPLIT on streams made by the test's own encoder (the pyarrow fixtures are in ReadTest). */
final class DeltaTest extends TestCase
{
    // ---- a DELTA_BINARY_PACKED encoder for the tests ----

    private static function wadd(int $a, int $b): int
    {
        $s = $a + $b;
        if (\is_int($s)) {
            return $s;
        }
        return $a > 0 ? ($a + \PHP_INT_MIN) + ($b + \PHP_INT_MIN) : ($a - \PHP_INT_MIN) + ($b - \PHP_INT_MIN);
    }

    private static function wsub(int $a, int $b): int
    {
        return self::wadd($a, $b === \PHP_INT_MIN ? \PHP_INT_MIN : -$b);
    }

    private static function wrap32(int $x): int
    {
        return (($x & 0xFFFFFFFF) ^ 0x80000000) - 0x80000000;
    }

    private static function bitWidth(int $u): int
    {
        if ($u < 0) {
            return 64;
        }
        $w = 0;
        while ($u > 0) {
            ++$w;
            $u >>= 1;
        }
        return $w;
    }

    /** @param list<int> $values (unsigned $bw-bit values, as PHP ints) */
    private static function pack(array $values, int $bw): string
    {
        $bits = '';
        foreach ($values as $v) {
            for ($i = 0; $i < $bw; $i++) {
                $bits .= (($v >> $i) & 1) ? '1' : '0';
            }
        }
        $out = '';
        for ($i = 0; $i < \strlen($bits); $i += 8) {
            $chunk = \strrev(\str_pad(\substr($bits, $i, 8), 8, '0'));
            $out .= \chr(\bindec($chunk));
        }
        return $out;
    }

    /**
     * @param list<int> $values
     * @param array{garbageWidths?: bool, unpadded?: bool} $opts
     */
    private static function encode(array $values, bool $is64, int $blockSize = 128, int $miniblocks = 4, array $opts = []): string
    {
        $n = \count($values);
        $out = Thrift::varint($blockSize) . Thrift::varint($miniblocks) . Thrift::varint($n) . Thrift::zigzag($n > 0 ? $values[0] : 0);
        $perMini = \intdiv($blockSize, $miniblocks);
        $deltas = [];
        for ($i = 1; $i < $n; $i++) {
            $d = self::wsub($values[$i], $values[$i - 1]);
            $deltas[] = $is64 ? $d : self::wrap32($d);
        }
        for ($b = 0; $b < \count($deltas); $b += $blockSize) {
            $block = \array_slice($deltas, $b, $blockSize);
            $min = \min($block);
            $rel = \array_map(static fn (int $d): int => self::wsub($d, $min), $block);
            $widths = '';
            $bodies = '';
            $used = (int) \ceil(\count($block) / $perMini);
            for ($m = 0; $m < $miniblocks; $m++) {
                if ($m >= $used) {
                    $widths .= \chr(($opts['garbageWidths'] ?? false) ? 255 : 0);
                    continue;
                }
                $slice = \array_slice($rel, $m * $perMini, $perMini);
                $w = 0;
                foreach ($slice as $r) {
                    $w = \max($w, self::bitWidth($r));
                }
                $widths .= \chr($w);
                if (!($opts['unpadded'] ?? false)) {
                    $slice = \array_pad($slice, $perMini, 0);
                }
                $bodies .= self::pack($slice, $w);
            }
            $out .= Thrift::zigzag($min) . $widths . $bodies;
        }
        return $out;
    }

    /** @param list<int> $values */
    private function roundTrip(array $values, bool $is64, int $blockSize = 128, int $miniblocks = 4, array $opts = []): void
    {
        $enc = self::encode($values, $is64, $blockSize, $miniblocks, $opts) . 'TRAILER';
        $p = 0;
        $got = Delta::binaryPacked($enc, $p, \strlen($enc), $is64, \count($values));
        $this->assertSame($values, $got, 'values');
        $this->assertSame(\strlen($enc) - 7, $p, 'position after the stream');
    }

    public function testRoundTripsOfEveryCountAndShape(): void
    {
        \mt_srand(42);
        $gens = [
            'sequence' => static fn (int $i): int => $i,
            'constant' => static fn (int $i): int => 7,
            'small' => static fn (int $i): int => \mt_rand(-3, 3),
            'wide' => static fn (int $i): int => [0, 1 << 62, -(1 << 62), 1 << 40, 12345][\mt_rand(0, 4)],
            'extreme' => static fn (int $i): int => $i % 2 ? \PHP_INT_MIN : \PHP_INT_MAX,
            'random64' => static fn (int $i): int => (\mt_rand() << 33) ^ (\mt_rand() << 2) ^ \mt_rand(0, 3) ^ (\mt_rand(0, 1) << 63),
        ];
        foreach ([0, 1, 2, 3, 127, 128, 129, 130, 255, 256, 257, 300, 513] as $n) {
            foreach ($gens as $name => $g) {
                $v = [];
                for ($i = 0; $i < $n; $i++) {
                    $v[] = $g($i);
                }
                $this->roundTrip($v, true);
                $v32 = \array_map(static fn (int $x): int => self::wrap32($x), $v);
                $this->roundTrip($v32, false);
            }
        }
    }

    public function testInt32WrapAround(): void
    {
        $v = [2147483647, -2147483648, 2147483647, -2147483648, 0, 2147483647, -1, -2147483648];
        $this->roundTrip($v, false);
        $this->roundTrip($v, false, 128, 1);
        // an INT32 stream whose deltas were computed in 64 bits (no wrap): the sum still lands on the right INT32
        $enc = Thrift::varint(128) . Thrift::varint(4) . Thrift::varint(3) . Thrift::zigzag(2147483647) . Thrift::zigzag(1) . "\x00\x00\x00\x00";
        $p = 0;
        $this->assertSame([2147483647, -2147483648, -2147483647], Delta::binaryPacked($enc, $p, \strlen($enc), false, 3));
    }

    public function testInt64WrapAroundNeverBecomesAFloat(): void
    {
        $v = [\PHP_INT_MAX, \PHP_INT_MIN, \PHP_INT_MAX, 0, \PHP_INT_MIN, \PHP_INT_MIN + 1, \PHP_INT_MAX - 1];
        $this->roundTrip($v, true);
        $p = 0;
        $enc = self::encode($v, true);
        foreach (Delta::binaryPacked($enc, $p, \strlen($enc), true, \count($v)) as $x) {
            $this->assertIsInt($x);
        }
    }

    public function testBlockAndMiniblockGeometries(): void
    {
        \mt_srand(3);
        $v = [];
        for ($i = 0; $i < 700; $i++) {
            $v[] = \mt_rand(-100000, 100000);
        }
        foreach ([[128, 1], [128, 2], [128, 4], [128, 8], [256, 4], [256, 8], [256, 2], [512, 16], [1024, 32]] as [$bs, $mb]) {
            $this->roundTrip($v, true, $bs, $mb);
            $this->roundTrip($v, false, $bs, $mb);
        }
    }

    public function testUnusedMiniblocksMayCarryAnyBitWidthAndTheLastOneNeedNotBePadded(): void
    {
        \mt_srand(9);
        foreach ([2, 5, 33, 70, 131, 200] as $n) {
            $v = [];
            for ($i = 0; $i < $n; $i++) {
                $v[] = \mt_rand(-5000, 5000);
            }
            $enc = self::encode($v, true, 128, 4, ['garbageWidths' => true]);
            $p = 0;
            $this->assertSame($v, Delta::binaryPacked($enc, $p, \strlen($enc), true, $n), "garbage widths, $n values");
            $enc = self::encode($v, true, 128, 4, ['unpadded' => true]);
            $p = 0;
            $this->assertSame($v, Delta::binaryPacked($enc, $p, \strlen($enc), true, $n), "unpadded, $n values");
        }
    }

    public function testHeaderThatDeclaresZeroValues(): void
    {
        $enc = self::encode([], true);
        $p = 0;
        $this->assertSame([], Delta::binaryPacked($enc, $p, \strlen($enc), true, 0));
        $this->assertSame(\strlen($enc), $p);
        $this->expectException(ParquetException::class);
        $p = 0;
        Delta::binaryPacked($enc, $p, \strlen($enc), true, 3);
    }

    /** @return array<string, array{string, int}> */
    public static function badDelta(): array
    {
        $ok = self::encode([1, 5, 9, 2, 100], true);
        return [
            'empty' => ['', 1],
            'count differs from the header' => [$ok, 4],
            'block size zero' => [Thrift::varint(0) . Thrift::varint(4) . Thrift::varint(3) . Thrift::zigzag(1), 3],
            'block size not a multiple of 128' => [Thrift::varint(100) . Thrift::varint(4) . Thrift::varint(3) . Thrift::zigzag(1), 3],
            'miniblocks do not divide the block' => [Thrift::varint(128) . Thrift::varint(3) . Thrift::varint(3) . Thrift::zigzag(1), 3],
            'zero miniblocks' => [Thrift::varint(128) . Thrift::varint(0) . Thrift::varint(3) . Thrift::zigzag(1), 3],
            'huge block size' => [Thrift::varint(1 << 40) . Thrift::varint(4) . Thrift::varint(3) . Thrift::zigzag(1), 3],
            'header only' => [Thrift::varint(128) . Thrift::varint(4) . Thrift::varint(3) . Thrift::zigzag(1), 3],
            'bit widths cut off' => [Thrift::varint(128) . Thrift::varint(4) . Thrift::varint(3) . Thrift::zigzag(1) . Thrift::zigzag(0) . "\x01\x01", 3],
            'miniblock cut off' => [Thrift::varint(128) . Thrift::varint(4) . Thrift::varint(3) . Thrift::zigzag(1) . Thrift::zigzag(0) . "\x08\x00\x00\x00" . "\x01", 3],
            'bit width above 64' => [Thrift::varint(128) . Thrift::varint(4) . Thrift::varint(3) . Thrift::zigzag(1) . Thrift::zigzag(0) . "\x41\x00\x00\x00" . \str_repeat("\xFF", 600), 3],
            'varint too long' => [\str_repeat("\x80", 12), 3],
        ];
    }

    /** @dataProvider badDelta */
    #[\PHPUnit\Framework\Attributes\DataProvider('badDelta')]
    public function testBadStreams(string $enc, int $count): void
    {
        $p = 0;
        $this->expectException(ParquetException::class);
        Delta::binaryPacked($enc, $p, \strlen($enc), true, $count);
    }

    public function testEveryTruncationOfAStreamIsAParquetException(): void
    {
        \mt_srand(1);
        $v = [];
        for ($i = 0; $i < 260; $i++) {
            $v[] = \mt_rand(-1000000, 1000000);
        }
        $enc = self::encode($v, true);
        for ($n = 0; $n < \strlen($enc) - 100; $n += 3) { // (the last miniblock holds 3 values: cutting its padding is still readable)
            $p = 0;
            try {
                Delta::binaryPacked(\substr($enc, 0, $n), $p, $n, true, \count($v));
                $this->fail("a stream cut to $n bytes decoded");
            } catch (ParquetException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    // ---- DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY ----

    /** @param list<string> $strings */
    private static function deltaLength(array $strings): string
    {
        return self::encode(\array_map('strlen', $strings), false) . \implode('', $strings);
    }

    public function testDeltaLengthByteArray(): void
    {
        $s = ['', 'a', "b\x00c", \str_repeat('z', 300), '', 'end'];
        $enc = self::deltaLength($s) . 'TAIL';
        $p = 0;
        $this->assertSame($s, Delta::lengthByteArray($enc, $p, \strlen($enc), \count($s)));
        $this->assertSame(\strlen($enc) - 4, $p);
        $p = 0;
        $this->assertSame([], Delta::lengthByteArray(self::deltaLength([]), $p, \strlen(self::deltaLength([])), 0));
    }

    public function testDeltaLengthByteArrayBadInput(): void
    {
        $enc = self::deltaLength(['abc', 'def']);
        foreach ([\substr($enc, 0, -1), self::encode([5, -1], false) . 'xxxx', self::encode([3, 3], false) . 'abc'] as $bad) {
            $p = 0;
            try {
                Delta::lengthByteArray($bad, $p, \strlen($bad), 2);
                $this->fail('accepted');
            } catch (ParquetException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testDeltaByteArray(): void
    {
        $values = ['https://a.org/1', 'https://a.org/2', 'https://a.org/22', 'https://b', '', 'x', 'x', "xy\x00"];
        $prefixes = [];
        $suffixes = [];
        $prev = '';
        foreach ($values as $v) {
            $k = 0;
            while ($k < \strlen($prev) && $k < \strlen($v) && $prev[$k] === $v[$k]) {
                ++$k;
            }
            $prefixes[] = $k;
            $suffixes[] = \substr($v, $k);
            $prev = $v;
        }
        $enc = self::encode($prefixes, false) . self::deltaLength($suffixes);
        $p = 0;
        $this->assertSame($values, Delta::byteArray($enc, $p, \strlen($enc), \count($values), null));
        $this->assertSame(\strlen($enc), $p);
    }

    public function testDeltaByteArrayRefusesAPrefixLongerThanThePreviousValueAndAWrongFixedLength(): void
    {
        $enc = self::encode([0, 9], false) . self::deltaLength(['abc', 'd']);
        $p = 0;
        try {
            Delta::byteArray($enc, $p, \strlen($enc), 2, null);
            $this->fail('accepted');
        } catch (ParquetException) {
            $this->addToAssertionCount(1);
        }
        $enc = self::encode([0, 1], false) . self::deltaLength(['abc', 'dx']);
        $p = 0;
        $this->assertSame(['abc', 'adx'], Delta::byteArray($enc, $p, \strlen($enc), 2, null));
        $p = 0;
        $this->expectException(ParquetException::class);
        Delta::byteArray($enc, $p, \strlen($enc), 2, 4); // fixed length 4, the values are 3 bytes
    }

    // ---- BYTE_STREAM_SPLIT ----

    public function testByteStreamSplit(): void
    {
        $split = static function (array $items): string {
            $w = \strlen($items[0]);
            $out = '';
            for ($k = 0; $k < $w; $k++) {
                foreach ($items as $it) {
                    $out .= $it[$k];
                }
            }
            return $out;
        };
        $f = [1.5, -2.25, 0.0, \unpack('g', \pack('g', 1e30))[1]];
        $this->assertSame($f, ByteStreamSplit::decode($split(\array_map(static fn ($x) => \pack('g', $x), $f)), 0, PhysicalType::Float, 4, null));
        $d = [1.5, -1e300, 5e-324];
        $this->assertSame($d, ByteStreamSplit::decode($split(\array_map(static fn ($x) => \pack('e', $x), $d)), 0, PhysicalType::Double, 3, null));
        $i32 = [1, -1, 2147483647, -2147483648, 0];
        $this->assertSame($i32, ByteStreamSplit::decode($split(\array_map(static fn ($x) => \pack('V', $x & 0xFFFFFFFF), $i32)), 0, PhysicalType::Int32, 5, null));
        $i64 = [1, -1, \PHP_INT_MAX, \PHP_INT_MIN];
        $this->assertSame($i64, ByteStreamSplit::decode($split(\array_map(static fn ($x) => \pack('P', $x), $i64)), 0, PhysicalType::Int64, 4, null));
        $fl = ['abc', "\x00\xff\x01", 'xyz'];
        $this->assertSame($fl, ByteStreamSplit::decode($split($fl), 0, PhysicalType::FixedLenByteArray, 3, 3));
        // an offset into the page, and zero values
        $this->assertSame([7], ByteStreamSplit::decode('JUNK' . \pack('V', 7), 4, PhysicalType::Int32, 1, null));
        $this->assertSame([], ByteStreamSplit::decode('', 0, PhysicalType::Double, 0, null));
    }

    public function testByteStreamSplitBadInput(): void
    {
        foreach ([[PhysicalType::Int32, 'abc', 1], [PhysicalType::Double, \str_repeat('a', 15), 2], [PhysicalType::ByteArray, 'abcd', 1], [PhysicalType::Boolean, 'a', 1], [PhysicalType::Float, '', \PHP_INT_MAX >> 3]] as [$t, $raw, $n]) {
            try {
                ByteStreamSplit::decode($raw, 0, $t, $n, 3);
                $this->fail("{$t->name} accepted");
            } catch (ParquetException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    // ---- BIT_PACKED definition levels over several pages ----

    public function testBitPackedLevelsOverSeveralPages(): void
    {
        // each page packs its own levels, most significant bit first, padded to a whole byte: 13 values (2 bytes), then 5 (1 byte), then 8 (1 byte)
        $levels = [[1, 0, 1, 1, 0, 0, 1, 1, 1, 0, 1, 0, 1], [0, 1, 1, 0, 1], [1, 1, 1, 1, 1, 1, 1, 0]];
        $chunk = '';
        $want = [];
        $next = 100;
        foreach ($levels as $bits) {
            $packed = '';
            for ($g = 0; $g < \intdiv(\count($bits) + 7, 8); $g++) {
                $byte = 0;
                for ($i = 0; $i < 8; $i++) {
                    $byte |= (($bits[$g * 8 + $i] ?? 0) & 1) << (7 - $i);
                }
                $packed .= \chr($byte);
            }
            $vals = '';
            foreach ($bits as $b) {
                if ($b) {
                    $vals .= \pack('V', $next);
                    $want[] = $next++;
                } else {
                    $want[] = null;
                }
            }
            $chunk .= FileBuilder::dataPageV1($packed . $vals, \count($bits), 0, null, null, [5 => [Thrift::STRUCT, [[1, Thrift::I32, \count($bits)], [2, Thrift::I32, 0], [3, Thrift::I32, 4], [4, Thrift::I32, 4]]]]);
        }
        $bytes = FileBuilder::file([['name' => 'c', 'type' => 1, 'optional' => true, 'chunk' => $chunk]], 26);
        $this->assertSame($want, Reader::fromString($bytes)->readColumn(0, 'c'));
    }
}
