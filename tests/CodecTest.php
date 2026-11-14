<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Codec, ParquetException, PhysicalType, UnsupportedException};
use Charm\Parquet\Internal\{Compression, PlainDecoder, RleHybrid, Snappy};
use Charm\Brotli\Brotli;
use Charm\Zstd\Zstd;
use PHPUnit\Framework\TestCase;

/** The building blocks on handwritten input: Snappy, the RLE/bit-packed hybrid, PLAIN values, page decompression. */
final class CodecTest extends TestCase
{
    public function testSnappyLiteralsAndCopies(): void
    {
        $this->assertSame('abcabcabc', Snappy::decode("\x09\x08abc\x16\x03\x00", 9)); // literal "abc", copy 6 bytes from 3 back (2-byte offset)
        $this->assertSame('abcdabcdabcd', Snappy::decode("\x0c\x0cabcd\x11\x04", 12)); // a copy of 8 bytes from 4 back with a 1-byte offset (tag ((8-4)<<2)|1)
        $this->assertSame('aaaaaaaaaa', Snappy::decode("\x0a\x00a\x15\x01", 10)); // an overlapping copy: 9 bytes from 1 back (tag ((9-4)<<2)|1)
    }

    public function testSnappyEmptyAndLongLiteral(): void
    {
        $this->assertSame('', Snappy::decode("\x00", 0));
        $lit = \str_repeat('x', 100);
        // literal longer than 60 bytes: tag 60<<2 with a 1-byte length (len - 1)
        $this->assertSame($lit, Snappy::decode("\x64" . "\xF0\x63" . $lit, 100));
    }

    public function testSnappyFourByteOffsetCopy(): void
    {
        $this->assertSame('abcdabcd', Snappy::decode("\x08\x0cabcd" . "\x0f\x04\x00\x00\x00", 8));
    }

    /** @return array<string, array{string, int}> */
    public static function badSnappy(): array
    {
        return [
            'no header' => ['', 3],
            'header only, size says 3' => ["\x03", 3],
            'declared size differs from the page header' => ["\x04\x0cabcd", 3],
            'literal runs past the data' => ["\x05\x10ab", 5],
            'literal past the declared size' => ["\x02\x0cabcd", 2],
            'copy with offset 0' => ["\x05\x00a\x01\x00", 5],
            'copy before the start' => ["\x05\x00a\x01\x05", 5],
            'copy past the declared size' => ["\x03\x00a\x16\x01\x00", 3],
            'cut copy tag' => ["\x04\x00a\x16\x01", 4],
            'cut 4-byte copy' => ["\x04\x00a\x0f\x01\x00", 4],
            'long literal length cut' => ["\x04\xF0", 4],
            'header varint too long' => ["\x80\x80\x80\x80\x80\x01", 1],
            'ends early' => ["\x05\x0cabcd", 5],
        ];
    }

    /** @dataProvider badSnappy */
    #[\PHPUnit\Framework\Attributes\DataProvider('badSnappy')]
    public function testBadSnappy(string $in, int $expected): void
    {
        $this->expectException(ParquetException::class);
        Snappy::decode($in, $expected);
    }

    public function testRleRunsAndBitPackedGroups(): void
    {
        // a run of 5 copies of value 3 (bit width 3 -> 1 byte), then one bit-packed group of 8 values 0..7 (3 bytes: 0b10001000, 0b11000110, 0b11111010)
        $raw = "\x0a\x03" . "\x03\x88\xC6\xFA";
        $max = -1;
        $this->assertSame([3, 3, 3, 3, 3, 0, 1, 2, 3, 4, 5, 6, 7], RleHybrid::decode($raw, 0, \strlen($raw), 3, 13, $max));
        $this->assertSame(7, $max);
    }

    public function testRleStopsAtTheRequestedCount(): void
    {
        $raw = "\xC8\x01\x05"; // a run of 100 copies of 5
        $max = 0;
        $this->assertSame([5, 5, 5], RleHybrid::decode($raw, 0, 3, 3, 3, $max));
        $this->assertSame(5, $max);
    }

    public function testRleBitWidthZeroAndOne(): void
    {
        $max = -1;
        $this->assertSame([0, 0, 0, 0], RleHybrid::decode("\x08", 0, 1, 0, 4, $max)); // a run of 4, no value bytes at width 0
        $this->assertSame([0, 0, 0, 0], RleHybrid::decode("\x03", 0, 1, 0, 4, $max)); // one bit-packed group of 8 at width 0, 4 taken
        $this->assertSame([1, 0, 1, 1, 0, 0, 0, 0], RleHybrid::decode("\x03\x0D", 0, 2, 1, 8, $max));
        $this->assertSame(1, $max);
    }

    public function testRleWideValues(): void
    {
        // width 20: a run of 2 copies of 0xABCDE (3 value bytes, little-endian)
        $max = 0;
        $this->assertSame([0xABCDE, 0xABCDE], RleHybrid::decode("\x04\xDE\xBC\x0A", 0, 4, 20, 2, $max));
        $this->assertSame(0xABCDE, $max);
    }

    /** @return array<string, array{string, int, int}> */
    public static function badRle(): array
    {
        return [
            'empty' => ['', 3, 1],
            'ends before the count' => ["\x04\x01", 5, 1],
            'bit-packed run past the data' => ["\x05\xFF", 16, 1],
            'run value cut off' => ["\x04", 2, 8],
            'header varint cut' => ["\x80", 2, 1],
            'header varint too long' => ["\x80\x80\x80\x80\x80\x80\x01", 2, 1],
            'bit width above 32' => ["\x04\x01", 2, 33],
        ];
    }

    /** @dataProvider badRle */
    #[\PHPUnit\Framework\Attributes\DataProvider('badRle')]
    public function testBadRle(string $raw, int $count, int $width): void
    {
        $max = 0;
        $this->expectException(ParquetException::class);
        RleHybrid::decode($raw, 0, \strlen($raw), $width, $count, $max);
    }

    public function testPlainFixedWidthTypes(): void
    {
        $this->assertSame([1, -2, 2147483647], PlainDecoder::decode(\pack('V*', 1, 0xFFFFFFFE, 0x7FFFFFFF), 0, PhysicalType::Int32, 3, null));
        $this->assertSame([\PHP_INT_MIN, -1, 5], PlainDecoder::decode(\pack('P*', \PHP_INT_MIN, -1, 5), 0, PhysicalType::Int64, 3, null));
        $this->assertSame([1.5, -0.25], PlainDecoder::decode(\pack('g*', 1.5, -0.25), 0, PhysicalType::Float, 2, null));
        $this->assertSame([1.5, -1e300], PlainDecoder::decode(\pack('e*', 1.5, -1e300), 0, PhysicalType::Double, 2, null));
        $this->assertSame(['abc', 'def'], PlainDecoder::decode('abcdef', 0, PhysicalType::FixedLenByteArray, 2, 3));
        $this->assertSame([\str_repeat('a', 12), \str_repeat('b', 12)], PlainDecoder::decode(\str_repeat('a', 12) . \str_repeat('b', 12), 0, PhysicalType::Int96, 2, null));
        $this->assertSame([true, false, true, true, false, false, false, false, true], PlainDecoder::decode("\x0D\x01", 0, PhysicalType::Boolean, 9, null));
        // an offset into the page
        $this->assertSame([7], PlainDecoder::decode("xx" . \pack('V', 7), 2, PhysicalType::Int32, 1, null));
    }

    public function testPlainByteArray(): void
    {
        $raw = \pack('V', 3) . 'abc' . \pack('V', 0) . \pack('V', 2) . "\x00\xff";
        $this->assertSame(['abc', '', "\x00\xff"], PlainDecoder::decode($raw, 0, PhysicalType::ByteArray, 3, null));
    }

    public function testPlainZeroValues(): void
    {
        foreach (PhysicalType::cases() as $t) {
            $this->assertSame([], PlainDecoder::decode('', 0, $t, 0, 4));
        }
    }

    public function testPlainShortInputIsAParquetException(): void
    {
        $bad = [
            [PhysicalType::Int32, 'abc', 1], [PhysicalType::Int64, \str_repeat('a', 15), 2], [PhysicalType::Float, 'abc', 1], [PhysicalType::Double, 'a', 1],
            [PhysicalType::Boolean, '', 1], [PhysicalType::FixedLenByteArray, 'abcde', 2], [PhysicalType::Int96, \str_repeat('a', 11), 1],
            [PhysicalType::ByteArray, "\x05\x00\x00\x00abc", 1], [PhysicalType::ByteArray, "\x01\x00", 1], [PhysicalType::ByteArray, "\xff\xff\xff\xff", 1],
            [PhysicalType::Int32, '', \PHP_INT_MAX >> 4],
        ];
        foreach ($bad as [$t, $raw, $n]) {
            try {
                PlainDecoder::decode($raw, 0, $t, $n, 3);
                $this->fail("{$t->name} accepted short input");
            } catch (ParquetException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testDecompressionChecksTheSize(): void
    {
        $data = \str_repeat('hello parquet ', 50);
        $this->assertSame($data, Compression::decompress(Codec::Uncompressed, $data, \strlen($data)));
        $this->assertSame($data, Compression::decompress(Codec::Gzip, \gzencode($data), \strlen($data)));
        $this->assertSame($data, Compression::decompress(Codec::Zstd, Zstd::compress($data), \strlen($data)));
        $this->assertSame($data, Compression::decompress(Codec::Brotli, Brotli::compress($data), \strlen($data)));
        foreach ([[Codec::Uncompressed, $data], [Codec::Gzip, \gzencode($data)], [Codec::Zstd, Zstd::compress($data)], [Codec::Brotli, Brotli::compress($data)]] as [$codec, $body]) {
            foreach ([\strlen($data) - 1, \strlen($data) + 1] as $wrong) {
                try {
                    Compression::decompress($codec, $body, $wrong);
                    $this->fail("{$codec->name} accepted a wrong expected size $wrong");
                } catch (ParquetException) {
                    $this->addToAssertionCount(1);
                }
            }
        }
    }

    public function testGzipWithSeveralMembers(): void
    {
        $a = \str_repeat('a', 5000);
        $b = \str_repeat('b', 7000);
        $this->assertSame($a . $b, Compression::decompress(Codec::Gzip, \gzencode($a) . \gzencode($b), 12000));
        $this->expectException(ParquetException::class);
        Compression::decompress(Codec::Gzip, \gzencode($a) . \substr(\gzencode($b), 0, -5), 12000);
    }

    public function testEmptyPageBodyIsEmptyForEveryCodec(): void
    {
        foreach ([Codec::Uncompressed, Codec::Snappy, Codec::Gzip, Codec::Zstd, Codec::Brotli] as $c) {
            $this->assertSame('', Compression::decompress($c, '', 0));
        }
    }

    public function testCorruptCompressedPages(): void
    {
        foreach ([Codec::Gzip, Codec::Zstd, Codec::Snappy, Codec::Brotli] as $c) {
            try {
                Compression::decompress($c, 'this is not compressed data', 10);
                $this->fail($c->name . ' accepted garbage');
            } catch (ParquetException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testZstdFailureIsAParquetExceptionWithTheCodecErrorAsPrevious(): void
    {
        try {
            Compression::decompress(Codec::Zstd, 'this is not compressed data', 10);
            $this->fail('accepted garbage');
        } catch (ParquetException $e) {
            $this->assertNotInstanceOf(\Charm\Parquet\UnsupportedException::class, $e);
            $this->assertInstanceOf(\RuntimeException::class, $e); // (UnexpectedValueException)
            $this->assertInstanceOf(\UnexpectedValueException::class, $e);
            $this->assertNotNull($e->getPrevious());
        }
    }

    public function testBrotliFailureIsAParquetException(): void
    {
        $this->expectException(ParquetException::class);
        Compression::decompress(Codec::Brotli, 'this is not compressed data', 10);
    }

    public function testZstdBombIsStoppedByTheDeclaredSize(): void
    {
        $bomb = Zstd::compress(\str_repeat("\0", 8 << 20));
        $this->assertLessThan(5000, \strlen($bomb));
        $this->expectException(ParquetException::class);
        Compression::decompress(Codec::Zstd, $bomb, 1000);
    }

    public function testBrotliBombIsStoppedByTheDeclaredSize(): void
    {
        // (the pure-PHP decoder refuses the meta-block before decoding it; ext-brotli stops one byte past the declared size)
        $bomb = Brotli::compress(\str_repeat("\0", 8 << 20));
        $this->assertLessThan(100, \strlen($bomb));
        $this->expectException(ParquetException::class);
        Compression::decompress(Codec::Brotli, $bomb, 1000);
    }

    public function testBrotliReadsLibbrotliStreams(): void
    {
        // made by libbrotli (quality 5 and 11) from 'hello parquet ' x 50
        $data = \str_repeat('hello parquet ', 50);
        foreach (['G7sCAMRjbF37Gj6iBumtHs/p0cBsrgE=', 'G7sC+MVtbF33dQgOoYqKBuMB0FhJobnzOgA='] as $b64) {
            $this->assertSame($data, Compression::decompress(Codec::Brotli, \base64_decode($b64), \strlen($data)));
        }
    }

    public function testUnsupportedCodecsNameThemselves(): void
    {
        try {
            Compression::decompress(Codec::Lzo, 'x', 1);
            $this->fail('no exception');
        } catch (UnsupportedException $e) {
            $this->assertSame('codec LZO', $e->getMessage());
        }
    }
}
