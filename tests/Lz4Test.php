<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Codec, FormatException};
use Charm\Parquet\Internal\{Compression, Lz4};
use PHPUnit\Framework\TestCase;

/** LZ4 block, Hadoop framing and frame, against streams made by the Python lz4 package (fixtures/vectors.json). */
final class Lz4Test extends TestCase
{
    /** @return list<array{plain: string, block: string, hadoop: string, frame: string}> */
    private static function vectors(): array
    {
        $v = \json_decode((string) \file_get_contents(__DIR__ . '/fixtures/vectors.json'), true, 512, \JSON_THROW_ON_ERROR);
        return \array_map(static fn (array $x): array => \array_map('hex2bin', $x), $v['lz4']);
    }

    public function testBlocksFromThePythonLibrary(): void
    {
        foreach (self::vectors() as $i => $v) {
            $this->assertSame($v['plain'], Lz4::block($v['block'], \strlen($v['plain'])), "vector $i");
            $this->assertSame($v['plain'], Compression::decompress(Codec::Lz4Raw, $v['block'], \strlen($v['plain'])), "vector $i via LZ4_RAW");
        }
    }

    public function testEveryFramingOfTheDeprecatedLz4Codec(): void
    {
        foreach (self::vectors() as $i => $v) {
            $n = \strlen($v['plain']);
            $this->assertSame($v['plain'], Compression::decompress(Codec::Lz4, $v['hadoop'], $n), "Hadoop framing, vector $i");
            $this->assertSame($v['plain'], Compression::decompress(Codec::Lz4, $v['block'], $n), "raw block, vector $i");
            $this->assertSame($v['plain'], Compression::decompress(Codec::Lz4, $v['frame'], $n), "frame, vector $i");
        }
    }

    public function testSeveralHadoopBlocks(): void
    {
        $vs = self::vectors();
        $in = $vs[2]['hadoop'] . $vs[4]['hadoop'];
        $this->assertSame($vs[2]['plain'] . $vs[4]['plain'], Compression::decompress(Codec::Lz4, $in, \strlen($vs[2]['plain'] . $vs[4]['plain'])));
    }

    public function testHandMadeBlocks(): void
    {
        // literals "abcd", then a match of 4+2 bytes at offset 4 ("abcdab"), then the final literals "xyz"
        $this->assertSame('abcdabcdabxyz', Lz4::block("\x42abcd\x04\x00\x30xyz", 13));
        // an overlapping match: literal "a", match length 4+11=15 at offset 1
        $this->assertSame(\str_repeat('a', 16), Lz4::block("\x1Ba\x01\x00", 16));
        // a match length with an extension byte: nibble 15 + byte 3 + 4 = 22 bytes at offset 1
        $this->assertSame(\str_repeat('a', 23), Lz4::block("\x1Fa\x01\x00\x03", 23));
        // literal length 15 + 3 (extension byte) = 18 literals only
        $this->assertSame(\str_repeat('q', 18), Lz4::block("\xF0\x03" . \str_repeat('q', 18), 18));
        $this->assertSame('', Lz4::block("\x00", 0));
    }

    /** @return array<string, array{string, int}> */
    public static function bad(): array
    {
        return [
            'literals run past the data' => ["\x50abc", 5],
            'literals above the expected size' => ["\x50abcde", 3],
            'offset cut off' => ["\x11a\x01", 5],
            'offset zero' => ["\x11a\x00\x00", 5],
            'offset before the start' => ["\x11a\x05\x00", 5],
            'match above the expected size' => ["\x1Fa\x01\x00\x20", 5],
            'size cut short' => ["\x10a", 5],
            'literal length cut off' => ["\xF0", 15],
            'match length cut off' => ["\x1Fa\x01\x00", 5],
            'empty with a size' => ['', 3],
        ];
    }

    /** @dataProvider bad */
    #[\PHPUnit\Framework\Attributes\DataProvider('bad')]
    public function testBadBlocks(string $in, int $expected): void
    {
        $this->expectException(FormatException::class);
        Lz4::block($in, $expected);
    }

    public function testWrongExpectedSizeEverywhere(): void
    {
        $v = self::vectors()[3];
        $n = \strlen($v['plain']);
        foreach ([[Codec::Lz4Raw, $v['block']], [Codec::Lz4, $v['hadoop']], [Codec::Lz4, $v['frame']]] as [$codec, $body]) {
            foreach ([$n - 1, $n + 1] as $wrong) {
                try {
                    Compression::decompress($codec, $body, $wrong);
                    $this->fail("{$codec->name} accepted size $wrong for $n");
                } catch (FormatException) {
                    $this->addToAssertionCount(1);
                }
            }
        }
    }

    public function testTruncatedAndFlippedStreamsNeverEscape(): void
    {
        \mt_srand(5);
        \set_error_handler(static function (int $no, string $msg, string $file, int $line): never {
            throw new \ErrorException($msg, 0, $no, $file, $line);
        });
        try {
            foreach (self::vectors() as $v) {
                $n = \strlen($v['plain']);
                foreach (['block', 'hadoop', 'frame'] as $kind) {
                    for ($i = 0; $i < 150; $i++) {
                        $c = $v[$kind];
                        if ($i % 3 === 0 && $c !== '') {
                            $c = \substr($c, 0, \mt_rand(0, \strlen($c) - 1));
                        } elseif ($c !== '') {
                            $p = \mt_rand(0, \strlen($c) - 1);
                            $c[$p] = \chr(\ord($c[$p]) ^ (1 << \mt_rand(0, 7)));
                        }
                        try {
                            $out = Compression::decompress($kind === 'block' ? Codec::Lz4Raw : Codec::Lz4, $c, $n);
                            $this->assertSame($n, \strlen($out));
                        } catch (FormatException) {
                            $this->addToAssertionCount(1);
                        }
                    }
                }
            }
        } finally {
            \restore_error_handler();
        }
    }
}
