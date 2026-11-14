<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Codec, Encoding, Reader, TimeUnit, Type, Writer};
use Charm\Parquet\Internal\Write\{RleEncoder, SnappyEncoder};
use Charm\Parquet\Internal\{RleHybrid, Snappy};
use PHPUnit\Framework\TestCase;

/** The encoders on their own (RLE hybrid, Snappy) and what the writer puts into the footer, the page layout and the indexes. */
final class WriterFormatTest extends TestCase
{
    // ---- RLE / bit-packed hybrid ----

    public function testRleRoundTripsRunsAroundTheEightValueBoundaries(): void
    {
        foreach ([1, 2, 3, 5, 8, 13, 20] as $width) {
            $max = (1 << $width) - 1;
            foreach ([1, 2, 3, 7, 8, 9, 15, 16, 17, 23, 24, 25, 64, 65] as $a) {
                foreach ([0, 1, 7, 8, 9, 16, 17] as $b) {
                    foreach ([0, 3, 8, 9] as $c) {
                        $vals = [];
                        for ($i = 0; $i < $a; $i++) {
                            $vals[] = $max;
                        }
                        for ($i = 0; $i < $b; $i++) {
                            $vals[] = $i % 2 === 0 ? 0 : $max;
                        }
                        for ($i = 0; $i < $c; $i++) {
                            $vals[] = 1 & $max;
                        }
                        $enc = RleEncoder::encode($vals, $width);
                        $m = 0;
                        $got = RleHybrid::decode($enc, 0, \strlen($enc), $width, \count($vals), $m);
                        $this->assertSame($vals, $got, "width $width runs $a/$b/$c");
                    }
                }
            }
        }
    }

    public function testRleIsCompactForLongRunsAndExactForShortOnes(): void
    {
        $this->assertSame("\x40\x01", RleEncoder::encode(\array_fill(0, 32, 1), 1));           // one RLE run: 32 << 1, the value
        $this->assertSame("\x10\x01", RleEncoder::encode(\array_fill(0, 8, 1), 1));            // 8 equal values: an RLE run (8 << 1), the value
        $this->assertSame("\x03\x7f", RleEncoder::encode(\array_fill(0, 7, 1), 1));            // 7: too short for a run, one packed group padded with a zero
        $this->assertSame("\x03\x05", RleEncoder::encode([1, 0, 1, 0, 0, 0, 0, 0], 1));        // packed
        $this->assertSame('', RleEncoder::encode([], 3));
        $this->assertSame("\x03\x05", RleEncoder::encode([1, 0, 1], 1));                       // the last group is padded with zeros
    }

    public function testRleRandomAgainstDecoder(): void
    {
        \mt_srand(77);
        for ($t = 0; $t < 300; $t++) {
            $w = \mt_rand(1, 12);
            $n = \mt_rand(0, 300);
            $vals = [];
            while (\count($vals) < $n) {
                $v = \mt_rand(0, (1 << $w) - 1);
                $run = \mt_rand(0, 3) === 0 ? \mt_rand(1, 40) : 1;
                for ($i = 0; $i < $run && \count($vals) < $n; $i++) {
                    $vals[] = $v;
                }
            }
            $enc = RleEncoder::encode($vals, $w);
            $m = 0;
            $this->assertSame($vals, RleHybrid::decode($enc, 0, \strlen($enc), $w, $n, $m), "trial $t width $w");
        }
    }

    // ---- Snappy ----

    /** @return array<string, string> */
    public static function snappyInputs(): array
    {
        \mt_srand(5);
        $random = '';
        for ($i = 0; $i < 70000; $i++) {
            $random .= \chr(\mt_rand(0, 255));
        }
        $words = '';
        for ($i = 0; $i < 20000; $i++) {
            $words .= ['alpha ', 'beta ', 'gamma ', 'delta ', 'epsilon '][\mt_rand(0, 4)];
        }
        return [
            'empty' => '', 'one byte' => 'a', 'three bytes' => 'abc', 'four equal' => 'aaaa',
            'run of 40' => \str_repeat('a', 40), 'run of 64' => \str_repeat('a', 64), 'run of 65' => \str_repeat('a', 65), 'run of 129' => \str_repeat('a', 129),
            'run of 100000' => \str_repeat('a', 100000),
            'pattern' => \str_repeat('abcdefgh', 5000),
            'literal 60' => \implode('', \array_map(static fn (int $i): string => \chr(65 + $i % 26) . \chr(97 + ($i * 7) % 26), \range(0, 29))),
            'random 70000' => $random,
            'words' => $words,
            'far copy' => $random . \substr($random, 100, 500) . $random,   // an offset beyond 65535 needs the 4-byte form
            'literal 61 then match' => \str_repeat('x', 3) . \substr($random, 0, 61) . \substr($random, 0, 61),
            'literal 256 then match' => \substr($random, 0, 256) . \substr($random, 0, 300),
            'literal 257 then match' => \substr($random, 0, 257) . \substr($random, 0, 300),
            'literal 65536 then match' => \substr($random, 0, 65536) . \substr($random, 0, 300),
            'literal 65537 then match' => \substr($random, 0, 65537) . \substr($random, 0, 300),
        ];
    }

    public function testSnappyRoundTripsWithThisPackagesDecoder(): void
    {
        foreach (self::snappyInputs() as $name => $in) {
            $c = SnappyEncoder::compress($in);
            $this->assertSame($in, Snappy::decode($c, \strlen($in)), $name);
        }
    }

    public function testSnappyCompressesRepetition(): void
    {
        $this->assertLessThan(5000, \strlen(SnappyEncoder::compress(\str_repeat('a', 100000))), 'a long run: 3 bytes per 64-byte copy');
        $this->assertLessThan(2000, \strlen(SnappyEncoder::compress(\str_repeat('abcdefgh', 5000))));
    }

    public function testSnappyCopiesAreAtMost64BytesAndOffsetsWithinTheOutput(): void
    {
        // walk the compressed stream of a long run and check every element the way a decoder with strict limits would
        $in = \str_repeat('ab', 50000);
        $c = SnappyEncoder::compress($in);
        $p = 0;
        $len = 0;
        $shift = 0;
        do {
            $b = \ord($c[$p++]);
            $len |= ($b & 0x7F) << $shift;
            $shift += 7;
        } while ($b & 0x80);
        $this->assertSame(100000, $len);
        $out = 0;
        while ($p < \strlen($c)) {
            $tag = \ord($c[$p++]);
            switch ($tag & 3) {
                case 0:
                    $l = $tag >> 2;
                    if ($l >= 60) {
                        $nb = $l - 59;
                        $l = 0;
                        for ($i = 0; $i < $nb; $i++) {
                            $l |= \ord($c[$p++]) << (8 * $i);
                        }
                    }
                    $p += $l + 1;
                    $out += $l + 1;
                    break;
                case 2:
                case 3:
                    $l = ($tag >> 2) + 1;
                    $off = ($tag & 3) === 2 ? \unpack('v', $c, $p)[1] : \unpack('V', $c, $p)[1];
                    $p += ($tag & 3) === 2 ? 2 : 4;
                    $this->assertLessThanOrEqual(64, $l);
                    $this->assertGreaterThan(0, $off);
                    $this->assertLessThanOrEqual($out, $off, 'a copy reaches back before the start');
                    $out += $l;
                    break;
                default:
                    $this->fail('a 1-byte-offset copy: not emitted by this encoder');
            }
        }
        $this->assertSame(100000, $out);
    }

    public function testSnappyGoldenBytes(): void
    {
        // checked against python-snappy's decompress in tests/tools/roundtrip.php; the bytes are fixed here so a change shows up
        $this->assertSame("\x05\x10hello", SnappyEncoder::compress('hello'));
        $this->assertSame("\x28\x00a\x9a\x01\x00", SnappyEncoder::compress(\str_repeat('a', 40)));
        $this->assertSame("\x17\x0cabcd\x3e\x04\x00\x08xyz", SnappyEncoder::compress('abcdabcdabcdabcdabcdxyz'));
    }

    // ---- the footer and the pages ----

    private function build(array $schema, array $rows, array $options = []): Reader
    {
        return Reader::fromString(WriterHelper::write($schema, $rows, $options));
    }

    public function testFooterMagicAndLengthAndCreatedBy(): void
    {
        $bytes = WriterHelper::write(['a' => Type::int32()], [['a' => 1]], ['keyValueMetadata' => ['k' => 'v', 'x' => 'y']]);
        $this->assertSame('PAR1', \substr($bytes, 0, 4));
        $this->assertSame('PAR1', \substr($bytes, -4));
        $len = \unpack('V', $bytes, \strlen($bytes) - 8)[1];
        $this->assertGreaterThan(0, $len);
        $this->assertLessThan(\strlen($bytes) - 12, $len);
        $r = Reader::fromString($bytes);
        $m = $r->metadata();
        $this->assertSame(['k' => 'v', 'x' => 'y'], $m->keyValueMetadata);
        $this->assertStringStartsWith('charm/parquet version', (string) $m->createdBy);
        $this->assertSame(2, $m->version);
    }

    public function testRowGroupsAreSplitByRowCountAndBytes(): void
    {
        $rows = \array_map(static fn (int $i): array => ['a' => $i, 's' => \str_repeat('x', 100)], \range(0, 24));
        $schema = ['a' => Type::int64(), 's' => Type::string()];
        $r = $this->build($schema, $rows, ['rowGroupRows' => 10]);
        $this->assertSame(3, $r->rowGroupCount());
        $this->assertSame([10, 10, 5], \array_map(static fn ($g) => $g->numRows, $r->metadata()->rowGroups));
        // 24 bytes of estimate per int plus 48 + 100 per string: one row is about 190 estimated bytes
        $r = $this->build($schema, $rows, ['rowGroupBytes' => 1000]);
        $sizes = \array_map(static fn ($g) => $g->numRows, $r->metadata()->rowGroups);
        $this->assertSame(25, \array_sum($sizes));
        $this->assertGreaterThan(2, \count($sizes));
        $this->assertSame(\range(0, 24), \array_column(WriterHelper::read(WriterHelper::write($schema, $rows, ['rowGroupBytes' => 1000])), 'a'));
    }

    public function testStatisticsOfEveryOrderingRule(): void
    {
        $schema = [
            'u64' => Type::uint64(), 'i64' => Type::int64(), 'u32' => Type::uint32(), 's' => Type::string(), 'dec' => Type::decimal(10, 2), 'big' => Type::decimal(30, 2),
            'd' => Type::double(), 'f' => Type::float(), 'b' => Type::boolean(), 'dt' => Type::date(), 'fx' => Type::fixed(2),
        ];
        $rows = [
            ['u64' => '18446744073709551615', 'i64' => -5, 'u32' => 4000000000, 's' => 'b', 'dec' => '-0.01', 'big' => '-0.01', 'd' => 0.0, 'f' => NAN, 'b' => true, 'dt' => '2020-01-02', 'fx' => "\x80\x00"],
            ['u64' => 5, 'i64' => 9, 'u32' => 3, 's' => 'B', 'dec' => '-100.00', 'big' => '-100000000000000000000.00', 'd' => -0.0, 'f' => 2.5, 'b' => false, 'dt' => '1969-12-31', 'fx' => "\x7f\xff"],
            ['u64' => null, 'i64' => null, 'u32' => null, 's' => null, 'dec' => '99999999.99', 'big' => '99999999999999999999.99', 'd' => null, 'f' => -1.5, 'b' => null, 'dt' => null, 'fx' => null],
            ['u64' => '9223372036854775808', 'i64' => 0, 'u32' => 7, 's' => 'é', 'dec' => '0.00', 'big' => '0.00', 'd' => 3.5, 'f' => null, 'b' => true, 'dt' => '2020-01-01', 'fx' => "\x00\x00"],
        ];
        $r = $this->build($schema, $rows);
        $st = static fn (string $c) => $r->statistics(0, $c);
        $this->assertSame(['5', '18446744073709551615', 1], [(string) $st('u64')->min, (string) $st('u64')->max, $st('u64')->nullCount]);
        $this->assertSame([-5, 9, 1], [$st('i64')->min, $st('i64')->max, $st('i64')->nullCount]);
        $this->assertSame([3, 4000000000], [$st('u32')->min, $st('u32')->max]);
        $this->assertSame(['B', 'é', 1], [$st('s')->min, $st('s')->max, $st('s')->nullCount]);
        $this->assertSame(['-100.00', '99999999.99'], [$st('dec')->min, $st('dec')->max]);
        $this->assertSame(['-100000000000000000000.00', '99999999999999999999.99'], [$st('big')->min, $st('big')->max]);
        // zero minimum is written as -0.0, a zero maximum as +0.0; here min is -0.0 (from 0.0 and -0.0) and the maximum 3.5
        $this->assertSame('8000000000000000', \bin2hex(\pack('E', $st('d')->min)), 'the minimum is -0.0, not 0.0');
        $this->assertSame(3.5, $st('d')->max);
        $this->assertSame([-1.5, 2.5, 1], [$st('f')->min, $st('f')->max, $st('f')->nullCount]);   // NaN is left out
        $this->assertSame([false, true], [$st('b')->min, $st('b')->max]);
        $this->assertSame(['1969-12-31', '2020-01-02'], [$st('dt')->min, $st('dt')->max]);
        $this->assertSame(["\x00\x00", "\x80\x00"], [$st('fx')->min, $st('fx')->max]);
        foreach (\array_keys($schema) as $c) {
            $this->assertTrue($st($c)->isMinExact, $c);
            $this->assertTrue($st($c)->isMaxExact, $c);
        }
    }

    public function testStatisticsOfAllZeroDoublesAndAllNullColumn(): void
    {
        $r = $this->build(['d' => Type::double(), 'n' => Type::int32(), 'f' => Type::float()], [['d' => 0.0, 'n' => null, 'f' => NAN], ['d' => 0.0, 'n' => null, 'f' => NAN]]);
        $this->assertSame('8000000000000000', \bin2hex(\pack('E', $r->statistics(0, 'd')->min)));
        $this->assertSame('0000000000000000', \bin2hex(\pack('E', $r->statistics(0, 'd')->max)));
        $this->assertNull($r->statistics(0, 'n')->min);
        $this->assertSame(2, $r->statistics(0, 'n')->nullCount);
        $this->assertNull($r->statistics(0, 'f')->min);
        $this->assertSame(0, $r->statistics(0, 'f')->nullCount);
    }

    public function testStatisticsCanBeSwitchedOff(): void
    {
        $r = $this->build(['a' => Type::int32()], [['a' => 1], ['a' => 2]], ['statistics' => false]);
        $this->assertNull($r->statistics(0, 'a'));
        $this->assertNull($r->columnIndex(0, 'a'));
        $this->assertNotNull($r->offsetIndex(0, 'a'));
        $r = $this->build(['a' => Type::int32()], [['a' => 1], ['a' => 2]], ['pageIndex' => false]);
        $this->assertNull($r->offsetIndex(0, 'a'));
        $this->assertNull($r->columnIndex(0, 'a'));
        $this->assertNotNull($r->statistics(0, 'a'));
    }

    public function testOffsetIndexFirstRowsAndColumnIndexPerPage(): void
    {
        $rows = [];
        for ($i = 0; $i < 100; $i++) {
            $rows[] = ['a' => $i, 'n' => $i % 10 === 0 ? null : $i * 2, 'l' => \array_fill(0, $i % 4, $i)];
        }
        $schema = ['a' => Type::int64()->required(), 'n' => Type::int64(), 'l' => Type::list(Type::int64())];
        $r = $this->build($schema, $rows, ['dataPageSize' => 80, 'codec' => Codec::Uncompressed]);
        foreach (['a', 'n', 'l.list.element'] as $c) {
            $oi = $r->offsetIndex(0, $c);
            $this->assertGreaterThan(3, \count($oi->pages), $c);
            $this->assertSame(0, $oi->pages[0]->firstRowIndex);
            $prev = -1;
            $endOfPrevious = 4;
            foreach ($oi->pages as $i => $p) {
                $this->assertGreaterThan($prev, $p->firstRowIndex, "$c page $i");
                $prev = $p->firstRowIndex;
                $this->assertGreaterThanOrEqual($endOfPrevious, $p->offset);
                $endOfPrevious = $p->offset + $p->compressedSize;
            }
        }
        // the column index of "a": page minimum is that page's first row, ascending boundary order, no nulls
        $ci = $r->columnIndex(0, 'a');
        $oi = $r->offsetIndex(0, 'a');
        $this->assertSame(\count($oi->pages), \count($ci->min));
        foreach ($oi->pages as $i => $p) {
            $this->assertSame($p->firstRowIndex, $ci->min[$i]);
            $this->assertSame($oi->endRow($i, 100) - 1, $ci->max[$i]);
        }
        $this->assertSame(\Charm\Parquet\BoundaryOrder::Ascending, $ci->boundaryOrder);
        // nulls: the per-page null counts add up to the chunk's
        $cin = $r->columnIndex(0, 'n');
        $this->assertSame(10, \array_sum($cin->nullCounts));
        $this->assertSame(10, $r->statistics(0, 'n')->nullCount);
    }

    public function testCandidateRangesUseThePageIndexOfOurOwnFile(): void
    {
        $rows = [];
        for ($i = 0; $i < 1000; $i++) {
            $rows[] = ['a' => $i, 'g' => (string) \intdiv($i, 100)];
        }
        $r = $this->build(['a' => Type::int64()->required(), 'g' => Type::string()], $rows, ['dataPageSize' => 200, 'rowGroupRows' => 400, 'codec' => Codec::Uncompressed]);
        $this->assertSame(3, $r->rowGroupCount());
        $ranges = $r->candidateRanges('a', 450, 470);
        $count = \array_sum(\array_column($ranges, 1));
        $this->assertGreaterThanOrEqual(21, $count);
        $this->assertLessThan(100, $count, 'the page index narrows the range inside the row group');
        $got = \iterator_to_array($r->rows(['a'], ranges: $ranges), false);
        $values = \array_column($got, 'a');
        $this->assertContains(450, $values);
        $this->assertContains(470, $values);
        $this->assertSame(\range(450, 470), \array_values(\array_filter($values, static fn ($v) => $v >= 450 && $v <= 470)));
        $this->assertSame([], $r->candidateRanges('a', 2000, 3000));
        $this->assertSame([[0, 1000]], $r->candidateRanges('a'));
    }

    public function testEncodingsAndPageTypesPerOption(): void
    {
        $rows = \array_map(static fn (int $i): array => ['a' => $i % 3, 'n' => $i % 5 === 0 ? null : 'v' . ($i % 4)], \range(0, 99));
        $schema = ['a' => Type::int32()->required(), 'n' => Type::string()];
        $enc = static function (Reader $r, string $c): array {
            return \array_map(static fn ($e) => $e->name, $r->metadata()->rowGroups[0]->columns[\array_search($c, ['a', 'n'], true)]->encodings);
        };
        $with = $this->build($schema, $rows);
        $names = $enc($with, 'n');
        $this->assertContains('RleDictionary', $names);
        $this->assertContains('Plain', $names);   // the dictionary page itself
        $this->assertContains('Rle', $names);     // the definition levels
        $this->assertNotContains('Rle', $enc($with, 'a'), 'a required flat column has no levels');
        $without = $this->build($schema, $rows, ['dictionary' => false]);
        $this->assertNotContains('RleDictionary', $enc($without, 'n'));
        $this->assertNull($without->metadata()->rowGroups[0]->columns[1]->dictionaryPageOffset);
        $this->assertNotNull($with->metadata()->rowGroups[0]->columns[1]->dictionaryPageOffset);
    }

    public function testDictionaryFallsBackToPlainAtAPageBoundaryAndEveryValueSurvives(): void
    {
        $rows = [];
        for ($i = 0; $i < 400; $i++) {
            $rows[] = ['s' => \sprintf('value-%05d', $i < 200 ? $i % 20 : $i)];
        }
        $schema = ['s' => Type::string()->required()];
        $bytes = WriterHelper::write($schema, $rows, ['dataPageSize' => 150, 'dictionaryPageSize' => 300, 'codec' => Codec::Uncompressed]);
        $r = Reader::fromString($bytes);
        $names = \array_map(static fn ($e) => $e->name, $r->metadata()->rowGroups[0]->columns[0]->encodings);
        $this->assertContains('RleDictionary', $names);
        $this->assertContains('Plain', $names);
        $this->assertSame(\array_column($rows, 's'), \array_column(WriterHelper::read($bytes), 's'));
        // a dictionary that never fits is not written at all
        $bytes = WriterHelper::write($schema, $rows, ['dictionaryPageSize' => 20]);
        $this->assertNull(Reader::fromString($bytes)->metadata()->rowGroups[0]->columns[0]->dictionaryPageOffset);
        $this->assertSame(\array_column($rows, 's'), \array_column(WriterHelper::read($bytes), 's'));
    }

    public function testLegacyConvertedTypesAreWrittenNextToTheLogicalTypes(): void
    {
        $r = $this->build([
            's' => Type::string(), 'd' => Type::date(), 'dec' => Type::decimal(9, 2), 'u8' => Type::uint8(), 'ts' => Type::timestamp(TimeUnit::Millis, true),
            'l' => Type::list(Type::int32()), 'm' => Type::map(Type::string(), Type::int32()),
        ], []);
        $by = [];
        foreach ($r->schema()->root->children as $c) {
            $by[$c->name] = $c;
        }
        $this->assertSame('Utf8', $by['s']->convertedType?->name);
        $this->assertSame('Date', $by['d']->convertedType?->name);
        $this->assertSame('Decimal', $by['dec']->convertedType?->name);
        $this->assertSame('Uint8', $by['u8']->convertedType?->name);
        $this->assertSame('TimestampMillis', $by['ts']->convertedType?->name);
        $this->assertSame('List', $by['l']->convertedType?->name);
        $this->assertSame('Map', $by['m']->convertedType?->name);
    }

    public function testASchemaReadFromAFileCanBeWrittenBack(): void
    {
        $schema = ['a' => Type::int64()->required(), 'l' => Type::list(Type::struct(['x' => Type::decimal(20, 3), 'y' => Type::timestamp(TimeUnit::Nanos)])), 'm' => Type::map(Type::string(), Type::list(Type::float()))];
        $rows = [['a' => 1, 'l' => [['x' => '1.500', 'y' => '2020-01-01T00:00:00.123456789'], null], 'm' => ['k' => [1.5, null]]], ['a' => 2, 'l' => null, 'm' => []]];
        $first = WriterHelper::write($schema, $rows);
        $src = Reader::fromString($first);
        $w = new Writer($src->schema());
        $out = '';
        foreach ($src->rows() as $row) {
            $out .= $w->encode($row);
        }
        $out .= $w->finish();
        $this->assertNull(WriterHelper::firstDifference(WriterHelper::read($first), WriterHelper::read($out)));
        $this->assertSame(WriterHelper::text($src->schema()->root), WriterHelper::text(Reader::fromString($out)->schema()->root));
    }

    public function testNullCountsOfPagesThatHoldOnlyNulls(): void
    {
        $rows = [];
        for ($i = 0; $i < 60; $i++) {
            $rows[] = ['a' => $i >= 20 && $i < 40 ? null : $i];
        }
        $r = $this->build(['a' => Type::int64()], $rows, ['dataPageSize' => 80, 'codec' => Codec::Uncompressed]);
        $ci = $r->columnIndex(0, 'a');
        $this->assertNotNull($ci);
        $this->assertContains(true, $ci->nullPages);
        $this->assertSame(20, \array_sum($ci->nullCounts));
        foreach ($ci->nullPages as $i => $isNull) {
            if ($isNull) {
                $this->assertGreaterThan(0, $ci->nullCounts[$i]);
                $this->assertNull($ci->min[$i]);
            }
        }
    }

    public function testRowRangesOnRepeatedColumnsWithTinyPages(): void
    {
        // pages of a repeated column must start at a record: a row range read selects whole pages through the offset index
        $schema = ['id' => Type::int64()->required(), 'l' => Type::list(Type::string()), 'm' => Type::map(Type::string(), Type::list(Type::int32()))];
        $rows = [];
        for ($i = 0; $i < 150; $i++) {
            $rows[] = ['id' => $i, 'l' => $i % 7 === 3 ? null : \array_fill(0, $i % 5, 'v' . $i), 'm' => $i % 4 === 0 ? [] : ['k' . $i => [$i, null, $i + 1]]];
        }
        foreach ([1, 2] as $version) {
            $bytes = WriterHelper::write($schema, $rows, ['dataPageSize' => 40, 'rowGroupRows' => 100, 'pageVersion' => $version, 'codec' => Codec::Uncompressed]);
            $r = Reader::fromString($bytes);
            $all = \iterator_to_array($r->rows(), false);
            $this->assertNull(WriterHelper::firstDifference(WriterHelper::normalizeRows($schema, $rows), $all));
            \mt_srand(11);
            for ($t = 0; $t < 40; $t++) {
                $off = \mt_rand(0, 149);
                $lim = \mt_rand(0, 30);
                $got = \iterator_to_array(Reader::fromString($bytes)->rows(offset: $off, limit: $lim), false);
                $this->assertSame(WriterHelper::text(\array_slice($all, $off, $lim)), WriterHelper::text($got), "v$version rows $off + $lim");
            }
        }
    }

    public function testDataPageV2HeaderCountsRecords(): void
    {
        $rows = [];
        for ($i = 0; $i < 90; $i++) {
            $rows[] = ['l' => $i % 6 === 0 ? [] : \array_fill(0, $i % 4 + 1, $i)];
        }
        $bytes = WriterHelper::write(['l' => Type::list(Type::int32())], $rows, ['dataPageSize' => 30, 'pageVersion' => 2, 'codec' => Codec::Uncompressed]);
        $r = Reader::fromString($bytes);
        $oi = $r->offsetIndex(0, 'l.list.element');
        $this->assertGreaterThan(2, \count($oi->pages));
        $totalRows = 0;
        foreach ($oi->pages as $i => $p) {
            $header = (new \Charm\Parquet\Thrift\CompactReader($bytes, $p->offset))->readStruct();
            $this->assertSame(3, $header[1], 'DATA_PAGE_V2');
            $v2 = $header[8];
            $this->assertSame($oi->endRow($i, 90) - $p->firstRowIndex, $v2[3], "page $i num_rows");
            $this->assertGreaterThanOrEqual($v2[3], $v2[1], 'entries per page are at least the rows');
            $totalRows += $v2[3];
        }
        $this->assertSame(90, $totalRows);
    }
}
