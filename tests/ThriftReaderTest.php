<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\ParquetException;
use Charm\Parquet\Thrift\CompactReader;
use PHPUnit\Framework\TestCase;

final class ThriftReaderTest extends TestCase
{
    public function testScalarsAndZigzag(): void
    {
        $bytes = Thrift::struct([
            [1, Thrift::I32, 0],
            [2, Thrift::I32, -1],
            [3, Thrift::I32, 2147483647],
            [4, Thrift::I32, -2147483648],
            [5, Thrift::I64, \PHP_INT_MAX],
            [6, Thrift::I64, \PHP_INT_MIN],
            [7, Thrift::I16, -300],
            [8, Thrift::BYTE, -5],
            [9, Thrift::BOOL_TRUE, true],
            [10, Thrift::BOOL_FALSE, false],
            [11, Thrift::DOUBLE, -2.5],
            [12, Thrift::BINARY, "a\x00\xff"],
            [13, Thrift::BINARY, ''],
        ]);
        $s = (new CompactReader($bytes))->readStruct();
        $this->assertSame([1 => 0, 2 => -1, 3 => 2147483647, 4 => -2147483648, 5 => \PHP_INT_MAX, 6 => \PHP_INT_MIN, 7 => -300, 8 => -5,
            9 => true, 10 => false, 11 => -2.5, 12 => "a\x00\xff", 13 => ''], $s);
    }

    public function testFieldIdsWithLargeGapsAndDecreasingIds(): void
    {
        $bytes = Thrift::struct([[1, Thrift::I32, 1], [100, Thrift::I32, 2], [40, Thrift::I32, 3], [41, Thrift::I32, 4]]);
        $this->assertSame([1 => 1, 100 => 2, 40 => 3, 41 => 4], (new CompactReader($bytes))->readStruct());
    }

    public function testListsSetsMapsAndNestedStructs(): void
    {
        $big = \range(0, 40);
        $bytes = Thrift::struct([
            [1, Thrift::LIST, [Thrift::I32, [1, -2, 3]]],
            [2, Thrift::LIST, [Thrift::I32, $big]], // more than 14 elements: the long list header
            [3, Thrift::SET, [Thrift::BINARY, ['x', 'y']]],
            [4, Thrift::MAP, [Thrift::BINARY, Thrift::I32, [['a', 1], ['b', 2]]]],
            [5, Thrift::MAP, [Thrift::BINARY, Thrift::I32, []]],
            [6, Thrift::STRUCT, [[1, Thrift::STRUCT, [[1, Thrift::I32, 7]]], [2, Thrift::LIST, [Thrift::STRUCT, [[[1, Thrift::I32, 8]], [[1, Thrift::I32, 9]]]]]]],
            [7, Thrift::LIST, [Thrift::BOOL_TRUE, [true, false, true]]],
        ]);
        $s = (new CompactReader($bytes))->readStruct();
        $this->assertSame([1, -2, 3], $s[1]);
        $this->assertSame($big, $s[2]);
        $this->assertSame(['x', 'y'], $s[3]);
        $this->assertSame([['a', 1], ['b', 2]], $s[4]);
        $this->assertSame([], $s[5]);
        $this->assertSame([1 => [1 => 7], 2 => [[1 => 8], [1 => 9]]], $s[6]);
        $this->assertSame([true, false, true], $s[7]);
    }

    public function testUnknownFieldsOfEveryTypeAreSkippedWithoutLosingPlace(): void
    {
        // a reader that does not know fields 50..60 still finds field 61 after them
        $bytes = Thrift::struct([
            [50, Thrift::STRUCT, [[1, Thrift::LIST, [Thrift::STRUCT, [[[1, Thrift::BINARY, 'deep']]]]]]],
            [51, Thrift::MAP, [Thrift::I32, Thrift::STRUCT, [[1, [[1, Thrift::I32, 1]]]]]],
            [52, Thrift::DOUBLE, 1.0],
            [60, Thrift::I64, 123],
            [61, Thrift::I32, 42],
        ]);
        $this->assertSame(42, (new CompactReader($bytes))->readStruct()[61]);
    }

    public function testPositionAfterStructAndStartOffset(): void
    {
        $struct = Thrift::struct([[1, Thrift::I32, 5]]);
        $r = new CompactReader('JUNK' . $struct . 'TAIL', 4);
        $this->assertSame([1 => 5], $r->readStruct());
        $this->assertSame(4 + \strlen($struct), $r->position());
    }

    public function testEveryTruncationOfAComplexStructIsAParquetException(): void
    {
        $bytes = Thrift::struct([
            [1, Thrift::LIST, [Thrift::STRUCT, [[[1, Thrift::BINARY, 'hello'], [2, Thrift::I64, 1 << 40]], [[1, Thrift::DOUBLE, 1.5]]]]],
            [2, Thrift::MAP, [Thrift::BINARY, Thrift::I32, [['a', 1]]]],
            [3, Thrift::BINARY, \str_repeat('z', 200)],
        ]);
        for ($n = 0; $n < \strlen($bytes); $n++) {
            try {
                (new CompactReader(\substr($bytes, 0, $n)))->readStruct();
                $this->fail("a struct cut to $n of " . \strlen($bytes) . ' bytes was read');
            } catch (ParquetException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testNestingDeeperThanTheLimitIsRefused(): void
    {
        $deep = "\x00";
        for ($i = 0; $i < CompactReader::MAX_DEPTH + 2; $i++) {
            $deep = "\x1C" . $deep . "\x00"; // field 1, type struct, then the nested struct, then its stop
        }
        $this->expectException(ParquetException::class);
        $this->expectExceptionMessageMatches('/nested deeper/');
        (new CompactReader($deep))->readStruct();
    }

    public function testNestingAtTheLimitIsAccepted(): void
    {
        $s = "\x00";
        for ($i = 1; $i < CompactReader::MAX_DEPTH; $i++) {
            $s = "\x1C" . $s . "\x00";
        }
        $this->assertIsArray((new CompactReader($s))->readStruct());
    }

    public function testListsNestedDeeperThanTheLimitAreRefused(): void
    {
        // a list of lists of lists ...: field 1 = list of 1 element of type list, repeated
        $bytes = "\x19";
        for ($i = 0; $i < 50; $i++) {
            $bytes .= "\x19";
        }
        $this->expectException(ParquetException::class);
        (new CompactReader($bytes . "\x00"))->readStruct();
    }

    public function testContainerThatAnnouncesMoreElementsThanBytesLeft(): void
    {
        $this->expectException(ParquetException::class);
        // field 1, list, long form (15), 2^28 elements of i32, and no data
        (new CompactReader("\x19\xF5" . Thrift::varint(1 << 28)))->readStruct();
    }

    public function testBinaryLongerThanTheData(): void
    {
        $this->expectException(ParquetException::class);
        (new CompactReader("\x18" . Thrift::varint(1 << 30) . 'abc'))->readStruct();
    }

    public function testNegativeLengthFromAnOverlongVarint(): void
    {
        $this->expectException(ParquetException::class);
        (new CompactReader("\x18" . \str_repeat("\xFF", 9) . "\x01"))->readStruct();
    }

    public function testVarintLongerThanTenBytes(): void
    {
        $this->expectException(ParquetException::class);
        (new CompactReader("\x15" . \str_repeat("\x80", 12) . "\x01\x00"))->readStruct();
    }

    public function testInvalidTypeNibbles(): void
    {
        foreach (["\x1D\x00", "\x1E\x00", "\x1F\x00"] as $bytes) { // field 1 with type 13, 14, 15
            try {
                (new CompactReader($bytes))->readStruct();
                $this->fail('type nibble accepted');
            } catch (ParquetException) {
                $this->addToAssertionCount(1);
            }
        }
        $this->expectException(ParquetException::class);
        (new CompactReader("\x19\x1D\x00"))->readStruct(); // a list with element type 13
    }

    public function testFieldIdOutOfRange(): void
    {
        foreach (["\x05" . Thrift::zigzag(40000) . Thrift::zigzag(1) . "\x00", "\x05" . Thrift::zigzag(-4) . Thrift::zigzag(1) . "\x00", "\x05" . Thrift::zigzag(\PHP_INT_MAX) . Thrift::zigzag(1) . "\x10\x00"] as $bytes) {
            try {
                (new CompactReader($bytes))->readStruct();
                $this->fail('accepted');
            } catch (ParquetException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testEmptyInput(): void
    {
        $this->expectException(ParquetException::class);
        (new CompactReader(''))->readStruct();
    }

    public function testStartOutsideTheBuffer(): void
    {
        $this->expectException(ParquetException::class);
        new CompactReader('abc', 5);
    }

    public function testRandomBytesNeverEscapeAsAnythingElse(): void
    {
        \mt_srand(77);
        \set_error_handler(static function (int $no, string $msg, string $file, int $line): never {
            throw new \ErrorException($msg, 0, $no, $file, $line);
        });
        try {
            for ($i = 0; $i < 3000; $i++) {
                $b = '';
                for ($k = \mt_rand(0, 60); $k > 0; $k--) {
                    $b .= \chr(\mt_rand(0, 255));
                }
                try {
                    (new CompactReader($b))->readStruct();
                } catch (ParquetException) {
                }
                $this->addToAssertionCount(1);
            }
        } finally {
            \restore_error_handler();
        }
    }
}
