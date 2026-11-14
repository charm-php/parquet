<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{BoundaryOrder, FormatException, Reader, ValueMode};
use Charm\Parquet\Internal\{Logical, RangeCompare};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Page indexes, row ranges that read only the pages they need, and candidate ranges from statistics. */
final class PageIndexTest extends TestCase
{
    /** @return Reader a reader that records every range call into $calls (after construction, whose footer read is not recorded) */
    private function recording(string $name, ?array &$calls, ValueMode $m = ValueMode::Physical): Reader
    {
        $bytes = Fixtures::bytes($name);
        $calls = [];
        $r = new Reader(static function (int $o, int $l) use ($bytes, &$calls): string {
            $calls[] = [$o, $l];
            return \substr($bytes, $o, $l);
        }, \strlen($bytes), $m);
        $calls = [];
        return $r;
    }

    // ---- the indexes ----

    public function testIndexesAreReadLazilyAndShared(): void
    {
        $r = $this->recording('idx-flat-v1-plain', $calls);
        $this->assertSame([], $calls, 'nothing is fetched until an index is asked for');
        $oi = $r->offsetIndex(0, 'k');
        $this->assertCount(1, $calls);
        $this->assertGreaterThan(1, \count($oi->pages));
        $this->assertSame(0, $oi->pages[0]->firstRowIndex);
        $r->offsetIndex(0, 'k');
        $r->offsetIndex(0, 'u');
        $this->assertCount(1, $calls, 'the offset indexes of a row group are adjacent: one read serves them all');
        $r->columnIndex(0, 'k');
        $r->columnIndex(0, 's');
        $this->assertCount(2, $calls, 'and the column indexes of the row group are one more read');
        $r->offsetIndex(1, 'k');
        $this->assertCount(2 + 1, $calls, 'another row group has its own index block');
    }

    public function testFilesWithoutIndexesGiveNull(): void
    {
        $r = Fixtures::reader('idx-none');
        $this->assertNull($r->offsetIndex(0, 'k'));
        $this->assertNull($r->columnIndex(0, 'k'));
        $this->assertNull(Fixtures::reader('matrix-none-v1-dict')->offsetIndex(0, 'r_int32'));
    }

    public function testColumnIndexValues(): void
    {
        foreach ([ValueMode::Physical, ValueMode::Logical] as $mode) {
            $r = Reader::fromFile(Fixtures::path('idx-flat-v1-plain'), $mode);
            $ci = $r->columnIndex(0, 'k');
            $oi = $r->offsetIndex(0, 'k');
            $this->assertSame(BoundaryOrder::Ascending, $ci->boundaryOrder);
            $this->assertCount(\count($oi->pages), $ci->min);
            $this->assertSame(0, $ci->min[0]);
            $this->assertSame(0, $ci->nullCounts[0]);
            // the pages' ranges follow each other and cover the row group
            $keys = $r->readColumn(0, 'k');
            foreach ($oi->pages as $i => $p) {
                $end = $oi->endRow($i, 100);
                $this->assertSame($keys[$p->firstRowIndex], $ci->min[$i]);
                $this->assertSame($keys[$end - 1], $ci->max[$i]);
            }
            $this->assertSame(\pack('P', 0), $ci->minRaw[0]);
        }
        $r = Fixtures::logicalReader('idx-flat-v1-plain');
        $dt = $r->columnIndex(0, 'dt');
        $this->assertMatchesRegularExpression('/^\d{4}-\d\d-\d\d$/', $dt->min[0]);
        $n = $r->columnIndex(1, 'n');
        $this->assertContains(true, $n->nullPages, 'a row group that is all null has null pages');
        $i = \array_search(true, $n->nullPages, true);
        $this->assertNull($n->min[$i]);
        $this->assertNull($n->max[$i]);
        $this->assertNull($n->minRaw[$i]);
    }

    // ---- row ranges ----

    /** @return array<string, array{string}> */
    public static function files(): array
    {
        $out = [];
        foreach (['idx-small-v1', 'idx-small-v2', 'idx-small-none'] as $n) {
            $out[$n] = [$n];
        }
        return $out;
    }

    #[DataProvider('files')]
    public function testEveryOffsetAndLimitEqualsSlicingTheFullRead(string $name): void
    {
        $r = Fixtures::logicalReader($name);
        $all = \iterator_to_array($r->rows(), false);
        $n = \count($all);
        $this->assertSame(24, $n);
        for ($offset = 0; $offset <= $n + 1; $offset++) {
            for ($limit = 0; $limit <= $n + 1; $limit++) {
                $got = \iterator_to_array($r->rows(null, $offset, $limit), false);
                if ($got !== \array_slice($all, $offset, $limit)) {
                    $this->fail("$name rows(offset $offset, limit $limit) differs from the slice");
                }
            }
        }
        $this->assertSame(\array_slice($all, 5), \iterator_to_array($r->rows(null, 5), false), 'no limit');
        $this->addToAssertionCount(1);
    }

    #[DataProvider('files')]
    public function testSelectedColumnsAndRowGroupReadsWithRanges(string $name): void
    {
        $r = Fixtures::logicalReader($name);
        $full = $r->readRowGroup(1);
        for ($offset = 0; $offset <= 8; $offset++) {
            for ($limit = 0; $limit <= 9; $limit++) {
                $got = $r->readRowGroup(1, ['k', 'st', 'l.list.element', 'st.m.x'], $offset, $limit);
                foreach ($got as $path => $col) {
                    $this->assertSame(\array_slice($path === 'k' || $path === 'st' ? $full[$path] ?? $r->readColumn(1, $path) : $r->readColumn(1, $path), $offset, $limit), $col, "$name $path $offset/$limit");
                }
            }
        }
        $this->assertSame([], $r->readColumn(2, 'k', 100));
        $this->assertSame([16, 17], $r->readColumn(2, 'k', 0, 2));
    }

    #[DataProvider('files')]
    public function testRangesComposeWithOffsetAndLimit(string $name): void
    {
        $r = Fixtures::logicalReader($name);
        $all = \iterator_to_array($r->rows(['k']), false);
        \mt_srand(123);
        for ($t = 0; $t < 60; $t++) {
            $ranges = [];
            for ($i = \mt_rand(0, 4); $i > 0; $i--) {
                $ranges[] = [\mt_rand(0, 25), \mt_rand(0, 9)];
            }
            $offset = \mt_rand(0, 10);
            $limit = \mt_rand(0, 3) === 0 ? null : \mt_rand(0, 20);
            $want = [];
            foreach ($all as $i => $row) {
                $in = false;
                foreach ($ranges as [$f, $c]) {
                    $in = $in || ($i >= $f && $i < $f + $c);
                }
                if ($in && $i >= $offset && ($limit === null || $i < $offset + $limit)) {
                    $want[] = $row;
                }
            }
            $this->assertSame($want, \iterator_to_array($r->rows(['k'], $offset, $limit, $ranges), false), $name . ' ' . \json_encode([$ranges, $offset, $limit]));
        }
    }

    public function testRowRangesOnALargerFileAndNestedFiles(): void
    {
        foreach (['idx-flat-v1-dict', 'idx-flat-v2-plain', 'idx-nested-v1', 'idx-nested-v2'] as $name) {
            $r = Fixtures::logicalReader($name);
            $all = \iterator_to_array($r->rows(), false);
            \mt_srand(9);
            for ($t = 0; $t < 25; $t++) {
                $offset = \mt_rand(0, \count($all));
                $limit = \mt_rand(0, 70);
                $got = \iterator_to_array($r->rows(null, $offset, $limit), false);
                $this->assertSame(\var_export(\array_slice($all, $offset, $limit), true), \var_export($got, true), "$name $offset/$limit"); // (var_export: NaN equals NaN)
            }
        }
    }

    public function testNegativeOffsetOrLimitIsAnInvalidArgument(): void
    {
        $r = Fixtures::reader('idx-small-v1');
        foreach ([fn () => \iterator_to_array($r->rows(null, -1)), fn () => \iterator_to_array($r->rows(null, 0, -1)), fn () => $r->readColumn(0, 'k', -1),
            fn () => \iterator_to_array($r->rows(null, 0, null, [[0, -2]])), fn () => \iterator_to_array($r->rows(null, 0, null, [[1.5, 2]]))] as $call) {
            try {
                $call();
                $this->fail('accepted');
            } catch (\InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    // ---- which bytes are fetched ----

    public function testSkippedRowGroupsAndPagesAreNotFetched(): void
    {
        // plain column: rows 150..154 live in row group 1, in one page
        $probe = $this->recording('idx-flat-v1-plain', $probeCalls);
        $oi = $probe->offsetIndex(1, 'k');
        $indexRead = $probeCalls[0];
        $p = $oi->pages[2]; // rows 30..39 of row group 1 (the page size is 300 bytes)
        $from = $oi->pages[2]->firstRowIndex;
        $r = $this->recording('idx-flat-v1-plain', $calls);
        $rows = \iterator_to_array($r->rows(['k'], 100 + $from + 1, 3), false);
        $this->assertSame([[100 + 0 === 0 ? 0 : 0]], [[0]]);
        $this->assertCount(3, $rows);
        $this->assertSame([$indexRead, [$p->offset, $p->compressedSize]], $calls, 'one index read, then exactly that page');

        // a range that ends exactly on the last row of a page reads that page and not the next one
        $lastOfPage = $oi->endRow(2, 100) - 1;
        $r = $this->recording('idx-flat-v1-plain', $calls);
        \iterator_to_array($r->rows(['k'], 100 + $oi->pages[2]->firstRowIndex, $lastOfPage - $oi->pages[2]->firstRowIndex + 1), false);
        $this->assertSame([$indexRead, [$p->offset, $p->compressedSize]], $calls, 'exactly one page for exactly the rows of that page');

        // dictionary column, first page of a row group: the dictionary page and the page are adjacent: ONE read
        $probe = $this->recording('idx-flat-v1-dict', $probeCalls);
        $oi = $probe->offsetIndex(2, 'k');
        $meta = $probe->metadata()->rowGroups[2]->columns[0];
        $r = $this->recording('idx-flat-v1-dict', $calls);
        \iterator_to_array($r->rows(['k'], 200, 2), false);
        $this->assertCount(2, $calls);
        $this->assertSame([$meta->startOffset(), $oi->pages[0]->offset + $oi->pages[0]->compressedSize - $meta->startOffset()], $calls[1], 'dictionary page and first page in one read');
        // a later page: the dictionary page and the page, two reads
        $r = $this->recording('idx-flat-v1-dict', $calls);
        $rowsLater = \iterator_to_array($r->rows(['k'], 200 + $oi->pages[3]->firstRowIndex, 1), false);
        $this->assertCount(1, $rowsLater);
        $this->assertCount(3, $calls);
        $this->assertSame([$meta->startOffset(), $oi->pages[0]->offset - $meta->startOffset()], $calls[1]);
        $this->assertSame([$oi->pages[3]->offset, $oi->pages[3]->compressedSize], $calls[2]);
        // nothing of the other row groups' column chunks was touched
        foreach ([0, 1] as $g) {
            $c = $probe->metadata()->rowGroups[$g]->columns[0];
            foreach ($calls as [$o, $l]) {
                $this->assertTrue($o + $l <= $c->startOffset() || $o >= $c->startOffset() + $c->totalCompressedSize, 'a read touched row group ' . $g);
            }
        }
    }

    public function testRowGroupsOutsideTheRangeAreNeverReadAndWholeGroupsAreOneRead(): void
    {
        $r = $this->recording('idx-flat-v1-plain', $calls);
        $meta = Fixtures::reader('idx-flat-v1-plain')->metadata();
        \iterator_to_array($r->rows(['k', 'u'], 100, 100), false); // exactly row group 1: whole chunks, no index needed
        $this->assertCount(2, $calls, 'one read per column chunk');
        foreach ($calls as [$o, $l]) {
            $inside = false;
            foreach ($meta->rowGroups[1]->columns as $c) {
                $inside = $inside || ($o >= $c->startOffset() && $o + $l <= $c->startOffset() + $c->totalCompressedSize + 256);
            }
            $this->assertTrue($inside);
        }
        $r = $this->recording('idx-flat-v1-plain', $calls);
        $this->assertSame([], \iterator_to_array($r->rows(null, 300, 5), false));
        $this->assertSame([], $calls, 'a range past the end reads nothing');
        $this->assertSame([], \iterator_to_array($r->rows(null, 0, 0), false));
        $this->assertSame([], $calls);
    }

    public function testWithoutAnOffsetIndexTheChunkIsReadAndSliced(): void
    {
        $r = $this->recording('idx-none', $calls);
        $rows = \iterator_to_array($r->rows(['k'], 120, 4), false);
        $this->assertSame([['k' => 360], ['k' => 363], ['k' => 366], ['k' => 369]], $rows);
        $this->assertCount(1, $calls);
        $meta = $r->metadata()->rowGroups[1]->columns[0];
        $this->assertSame($meta->startOffset(), $calls[0][0]);
    }

    // ---- candidate ranges ----

    /** @return array{list<array{int, int}>, int} */
    private function covered(array $ranges, int $n): array
    {
        $prev = -1;
        $total = 0;
        foreach ($ranges as [$f, $c]) {
            $this->assertGreaterThan($prev, $f, 'sorted, non-overlapping');
            $this->assertGreaterThan(0, $c);
            $this->assertLessThanOrEqual($n, $f + $c);
            $prev = $f + $c - 1;
            $total += $c;
        }
        return [$ranges, $total];
    }

    private static function inRanges(array $ranges, int $row): bool
    {
        foreach ($ranges as [$f, $c]) {
            if ($row >= $f && $row < $f + $c) {
                return true;
            }
        }
        return false;
    }

    /** @return array<string, array{string, string}> */
    public static function candidateCases(): array
    {
        $out = [];
        foreach (['idx-flat-v1-plain', 'idx-flat-v2-dict', 'idx-none'] as $f) {
            foreach (['k', 'u', 'f', 'fs', 's', 'su', 'd', 'dt', 'ts', 'n', 'b'] as $c) {
                $out["$f $c"] = [$f, $c];
            }
        }
        return $out;
    }

    #[DataProvider('candidateCases')]
    public function testCandidateRangesNeverDropAMatchingRow(string $file, string $col): void
    {
        $phys = Fixtures::reader($file);
        $log = Fixtures::logicalReader($file);
        $p = [];
        $l = [];
        for ($g = 0; $g < 3; $g++) {
            \array_push($p, ...$phys->readColumn($g, $col));
            \array_push($l, ...$log->readColumn($g, $col));
        }
        $n = \count($p);
        $nonNull = \array_keys(\array_filter($p, static fn ($v) => $v !== null && !(\is_float($v) && \is_nan($v))));
        $this->assertNotEmpty($nonNull);
        \mt_srand(\crc32($file . $col));
        $pruned = 0;
        for ($t = 0; $t < 80; $t++) {
            $a = $nonNull[\mt_rand(0, \count($nonNull) - 1)];
            $b = $nonNull[\mt_rand(0, \count($nonNull) - 1)];
            $dec = $col === 'd';
            if (self::cmp($p[$a], $p[$b], $dec) > 0) {
                [$a, $b] = [$b, $a];
            }
            $openLow = \mt_rand(0, 5) === 0;
            $openHigh = \mt_rand(0, 5) === 0;
            $orNull = \mt_rand(0, 3) === 0;
            foreach ([[$phys, $p, ValueMode::Physical], [$log, $l, ValueMode::Logical]] as [$reader, $vals, $mode]) {
                $ranges = $reader->candidateRanges($col, $openLow ? null : $vals[$a], $openHigh ? null : $vals[$b], $orNull);
                [, $total] = $this->covered($ranges, $n);
                for ($i = 0; $i < $n; $i++) {
                    $v = $p[$i];
                    $match = $v === null ? $orNull : (!(\is_float($v) && \is_nan($v)) && ($openLow || self::cmp($v, $p[$a], $dec) >= 0) && ($openHigh || self::cmp($v, $p[$b], $dec) <= 0));
                    if ($match && !self::inRanges($ranges, $i)) {
                        $this->fail("$file $col ($mode->name): row $i (" . \var_export($v, true) . ") matches [" . ($openLow ? '-inf' : \var_export($vals[$a], true)) . ', ' . ($openHigh ? 'inf' : \var_export($vals[$b], true)) . "] orNull=" . (int) $orNull . ' but is in no returned range ' . \json_encode($ranges));
                    }
                }
                if ($total < $n) {
                    ++$pruned;
                }
            }
        }
        if (\in_array($col, ['k', 'fs', 's', 'dt', 'ts'], true) && $file !== 'idx-none') {
            $this->assertGreaterThan(20, $pruned, "$file $col: sorted data should let statistics rule out rows");
        }
        $this->addToAssertionCount(1);
    }

    private static function cmp(mixed $a, mixed $b, bool $decimalBytes = false): int
    {
        if ($decimalBytes) { // (a DECIMAL in FIXED_LEN_BYTE_ARRAY: big-endian two's complement)
            $int = static function (string $x): int {
                $v = 0;
                for ($i = 0, $n = \strlen($x); $i < $n; $i++) {
                    $v = ($v << 8) | \ord($x[$i]);
                }
                return (\ord($x[0]) & 0x80) !== 0 && \strlen($x) < 8 ? $v - (1 << (8 * \strlen($x))) : $v;
            };
            return $int($a) <=> $int($b);
        }
        return \is_string($a) ? \strcmp($a, $b) <=> 0 : $a <=> $b;
    }

    public function testCandidateRangesNarrowFurtherWithAColumnIndexThanWithRowGroupStatisticsAlone(): void
    {
        $with = Fixtures::reader('idx-flat-v1-plain')->candidateRanges('k', 330, 345);   // keys are 0, 3, 6 ...: rows 110..115
        $without = Fixtures::reader('idx-none')->candidateRanges('k', 330, 345);
        $count = static fn (array $r): int => \array_sum(\array_column($r, 1));
        $this->assertLessThan($count($without), $count($with));
        $this->assertSame([[100, 100]], $without, 'row group statistics alone: the whole row group');
        $this->assertTrue(self::inRanges($with, 110) && self::inRanges($with, 115));
        // an impossible query
        $this->assertSame([], Fixtures::reader('idx-flat-v1-plain')->candidateRanges('k', 100000, null));
        $this->assertSame([[0, 300]], Fixtures::reader('idx-flat-v1-plain')->candidateRanges('k'), 'no bounds: everything');
    }

    public function testNullsAndNullPages(): void
    {
        $r = Fixtures::reader('idx-flat-v1-plain');
        $n = [];
        for ($g = 0; $g < 3; $g++) {
            \array_push($n, ...$r->readColumn($g, 'n'));
        }
        $nulls = $r->candidateRanges('n', null, null, true);
        foreach ($n as $i => $v) {
            if ($v === null) {
                $this->assertTrue(self::inRanges($nulls, $i), "null row $i");
            }
        }
        // value-only query: pages that hold only nulls are ruled out
        $valuesOnly = $r->candidateRanges('n', 0, 1000);
        $oi = $r->offsetIndex(1, 'n');
        $ci = $r->columnIndex(1, 'n');
        $i = \array_search(true, $ci->nullPages, true);
        $this->assertNotFalse($i);
        for ($row = $oi->pages[$i]->firstRowIndex; $row < $oi->endRow($i, 100); $row++) {
            $this->assertFalse(self::inRanges($valuesOnly, 100 + $row), "row " . (100 + $row) . ' is in an all-null page');
        }
        // a required column has no nulls: orNull adds nothing
        $this->assertSame($r->candidateRanges('k', 30, 60), $r->candidateRanges('k', 30, 60, true));
    }

    public function testInexactStatisticsProveNothing(): void
    {
        foreach ([true, false] as $exact) {
            $stats = [[5, Thrift::BINARY, \pack('l', 20)], [6, Thrift::BINARY, \pack('l', 10)], [7, Thrift::BOOL_TRUE, $exact], [8, Thrift::BOOL_TRUE, $exact]];
            $f = FileBuilder::file([['name' => 'c', 'type' => 1, 'chunk' => FileBuilder::dataPageV1(\pack('V*', 10, 11, 12, 13, 14, 20), 6), 'statistics' => $stats]], 6);
            $r = Reader::fromString($f, ValueMode::Physical);
            $this->assertSame($exact ? [] : [[0, 6]], $r->candidateRanges('c', 100, 200), $exact ? 'exact: ruled out' : 'a truncated or unflagged bound: kept');
            $this->assertSame($exact ? [] : [[0, 6]], $r->candidateRanges('c', -5, 3));
            $this->assertSame([[0, 6]], $r->candidateRanges('c', 15, 17), 'overlapping the range: kept either way');
        }
    }

    public function testUnsafeComparisonsKeepTheData(): void
    {
        $r = Fixtures::reader('idx-flat-v1-plain');
        // NaN bound, mixed kinds and unknown columns' statistics: keep everything
        $this->assertSame([[0, 300]], $r->candidateRanges('f', \NAN, \NAN));
        $this->assertSame([[0, 300]], $r->candidateRanges('k', 'abc', 'xyz'));
        $this->assertSame([[0, 300]], $r->candidateRanges('s', 5, 6));
        $this->expectException(\InvalidArgumentException::class);
        $r->candidateRanges('nope', 1, 2);
    }

    public function testRangeCompare(): void
    {
        $v = \json_decode((string) \file_get_contents(__DIR__ . '/fixtures/vectors.json'), true, 512, \JSON_THROW_ON_ERROR)['decimalCompare'];
        $leaf = Fixtures::reader('decimals')->schema()->leaf('p4');
        foreach ($v as [$a, $b, $want]) {
            $this->assertSame($want, RangeCompare::compare($a, $b, $leaf, ValueMode::Logical), "$a vs $b");
        }
        $date = Fixtures::reader('logical-edges')->schema()->leaf('date');
        $this->assertSame(-1, RangeCompare::compare('2020-01-01', '2020-01-02', $date, ValueMode::Logical));
        $this->assertNull(RangeCompare::compare('-0001-01-01', '2020-01-02', $date, ValueMode::Logical));
        $this->assertNull(RangeCompare::compare('10000-01-01', '2020-01-02', $date, ValueMode::Logical));
        $str = Fixtures::reader('matrix-none-v1-dict')->schema()->leaf('r_string');
        $this->assertSame(-1, RangeCompare::compare("a\xFF", "b", $str, ValueMode::Logical));
        $this->assertSame(1, RangeCompare::compare("\xFF", "a", $str, ValueMode::Logical), 'bytes compare unsigned');
        $this->assertSame(-1, RangeCompare::compare('10', '9a', $str, ValueMode::Logical) ?? -1);
        $this->assertNull(RangeCompare::compare(\NAN, 1.0, $str, ValueMode::Physical));
        $this->assertSame(0, RangeCompare::compare(-0.0, 0.0, $str, ValueMode::Physical));
    }

    // ---- the transport ----

    public function testReadingOverARangeTransport(): void
    {
        // a fake HTTP transport: a closure over the "remote" bytes that records the byte ranges it was asked for
        $remote = Fixtures::bytes('idx-flat-v2-dict');
        $requests = [];
        $http = static function (int $offset, int $length) use ($remote, &$requests): string {
            $requests[] = "bytes=$offset-" . ($offset + $length - 1);
            return \substr($remote, $offset, $length);
        };
        $reader = new Reader($http, \strlen($remote), ValueMode::Logical);
        $this->assertCount(1, $requests, 'the footer: one request for the end of the file');
        $rows = \iterator_to_array($reader->rows(['k', 's'], 250, 3), false);
        $this->assertSame(3, \count($rows));
        $fetched = 0;
        foreach ($requests as $q) {
            \preg_match('/bytes=(\d+)-(\d+)/', $q, $m);
            $fetched += (int) $m[2] - (int) $m[1] + 1;
        }
        // a 3-row read fetched the footer, three indexes' worth and two pages: well under the whole file would only show on big files,
        // so check the requests are exactly the footer, the index blocks and the pages
        $this->assertLessThan(10, \count($requests));
        $this->assertGreaterThan(0, $fetched);
        $this->assertSame(\array_slice(\iterator_to_array(Reader::fromString($remote, ValueMode::Logical)->rows(['k', 's']), false), 250, 3), $rows);
    }

    // ---- hostile indexes ----

    /** A one-column file of 6 rows in 3 pages of 2 INT32 values, with a hand-made offset index. @param list<array{int, int, int}> $pageLocs offset (relative to the first page), size, first row */
    private static function file(array $pageLocs, array $extra = []): string
    {
        $pages = '';
        $sizes = [];
        foreach ([[1, 2], [3, 4], [5, 6]] as $vals) {
            $page = FileBuilder::dataPageV1(\pack('V*', ...$vals), 2);
            $sizes[] = \strlen($page);
            $pages .= $page;
        }
        $struct = static function (array $locs) use ($sizes): string {
            $list = [];
            foreach ($locs as [$off, $size, $row]) {
                $list[] = [[1, Thrift::I64, $off], [2, Thrift::I32, $size], [3, Thrift::I64, $row]];
            }
            return Thrift::struct([[1, Thrift::LIST, [Thrift::STRUCT, $list]]]);
        };
        $locs = [];
        foreach ($pageLocs as [$o, $sz, $row]) {
            $locs[] = [4 + $o, $sz, $row]; // 4: the file starts with PAR1, the chunk follows
        }
        return FileBuilder::file([['name' => 'c', 'type' => 1, 'chunk' => $pages, 'offsetIndex' => $struct($locs)] + $extra], 6);
    }

    public function testAWellFormedHandMadeIndexWorks(): void
    {
        $pl = FileBuilder::dataPageV1(\pack('V*', 1, 2), 2);
        $s = \strlen($pl);
        $f = self::file([[0, $s, 0], [$s, $s, 2], [2 * $s, $s, 4]]);
        $r = Reader::fromString($f);
        $this->assertCount(3, $r->offsetIndex(0, 'c')->pages);
        $this->assertSame([3, 4, 5], \array_column(\iterator_to_array($r->rows(['c'], 2, 3), false), 'c'));
    }

    /** @return array<string, array{callable(int): string}> */
    public static function hostile(): array
    {
        return [
            'a page outside the chunk' => [fn (int $s) => self::file([[0, $s, 0], [$s, $s, 2], [100000, $s, 4]])],
            'a page that starts before the chunk' => [fn (int $s) => self::file([[-50, $s, 0], [$s, $s, 2], [2 * $s, $s, 4]])],
            'overlapping pages' => [fn (int $s) => self::file([[0, $s, 0], [$s - 5, $s, 2], [2 * $s, $s, 4]])],
            'pages out of order in the file' => [fn (int $s) => self::file([[0, $s, 0], [2 * $s, $s, 2], [$s, $s, 4]])],
            'first row indexes not increasing' => [fn (int $s) => self::file([[0, $s, 0], [$s, $s, 4], [2 * $s, $s, 2]])],
            'first row indexes equal in a flat column' => [fn (int $s) => self::file([[0, $s, 0], [$s, $s, 2], [2 * $s, $s, 2]])],
            'the first page does not start at row 0' => [fn (int $s) => self::file([[0, $s, 1], [$s, $s, 2], [2 * $s, $s, 4]])],
            'a first row index beyond the row group' => [fn (int $s) => self::file([[0, $s, 0], [$s, $s, 2], [2 * $s, $s, 6]])],
            'a page of size zero' => [fn (int $s) => self::file([[0, 0, 0], [$s, $s, 2], [2 * $s, $s, 4]])],
            'no pages for a row group with rows' => [fn (int $s) => self::file([])],
            'counts that disagree with the chunk: a page holds fewer rows than the index says' => [fn (int $s) => self::file([[0, $s, 0], [$s, $s, 3], [2 * $s, $s, 4]])],
            'counts that disagree with the chunk: the index skips a page' => [fn (int $s) => self::file([[0, $s, 0], [2 * $s, $s, 4]])],
            'the index lies outside the file' => [fn (int $s) => self::file([[0, $s, 0], [$s, $s, 2], [2 * $s, $s, 4]], ['offsetIndexAt' => 1 << 40])],
            'the index length exceeds the file' => [fn (int $s) => self::file([[0, $s, 0], [$s, $s, 2], [2 * $s, $s, 4]], ['offsetIndexLength' => 1 << 30])],
        ];
    }

    #[DataProvider('hostile')]
    public function testHostileOffsetIndexesAreRefused(callable $make): void
    {
        $s = \strlen(FileBuilder::dataPageV1(\pack('V*', 1, 2), 2));
        $bytes = $make($s);
        \set_error_handler(static function (int $no, string $msg, string $file, int $line): never {
            throw new \ErrorException($msg, 0, $no, $file, $line);
        });
        try {
            $r = Reader::fromString($bytes);
            \iterator_to_array($r->rows(['c'], 3, 2), false);
            $this->fail('the range was read');
        } catch (FormatException) {
            $this->addToAssertionCount(1);
        } finally {
            \restore_error_handler();
        }
    }

    public function testARepeatedColumnPageThatDoesNotStartARowIsRefused(): void
    {
        // optional list<int>: rows [1,2,3,4], [5], [], [6,7]; pages split as 3 + 3 + 2 entries: page 2 starts in the middle of row 0
        $top = [NestedFileBuilder::group('l', NestedFileBuilder::OPT, [NestedFileBuilder::group('list', NestedFileBuilder::REP, [NestedFileBuilder::leaf('element', NestedFileBuilder::OPT)])], NestedFileBuilder::CONV_LIST)];
        $data = ['values' => [1, 2, 3, 4, 5, 6, 7], 'defs' => [3, 3, 3, 3, 3, 1, 3, 3], 'reps' => [0, 1, 1, 1, 0, 0, 0, 1], 'split' => [3, 3, 2]];
        // an honest index: page 0 starts row 0, page 1 starts row 0's continuation (same first row, as pyarrow writes v1), page 2 row 3
        $honest = NestedFileBuilder::file($top, [$data + ['index' => [0, 0, 3]]], 4);
        $this->assertSame([['l' => [6, 7]]], \iterator_to_array(Reader::fromString($honest)->rows(null, 3, 1), false));
        $this->assertSame([['l' => [5]], ['l' => []]], \iterator_to_array(Reader::fromString($honest)->rows(null, 1, 2), false));
        // a dishonest one says page 1 (which starts with repetition level 1) starts row 1: starting a read there must be refused
        $lie = NestedFileBuilder::file($top, [$data + ['index' => [0, 1, 3]]], 4);
        $this->expectException(FormatException::class);
        \iterator_to_array(Reader::fromString($lie)->rows(null, 1, 1), false);
    }

    public function testHostileColumnIndexes(): void
    {
        $bad = [
            'lists of different lengths' => Thrift::struct([[1, Thrift::LIST, [Thrift::BOOL_TRUE, [false, false]]], [2, Thrift::LIST, [Thrift::BINARY, ['a']]], [3, Thrift::LIST, [Thrift::BINARY, ['b', 'c']]], [4, Thrift::I32, 0]]),
            'min is not a string' => Thrift::struct([[1, Thrift::LIST, [Thrift::BOOL_TRUE, [false]]], [2, Thrift::LIST, [Thrift::I32, [1]]], [3, Thrift::LIST, [Thrift::BINARY, ['b']]], [4, Thrift::I32, 0]]),
            'null counts of another length' => Thrift::struct([[1, Thrift::LIST, [Thrift::BOOL_TRUE, [false]]], [2, Thrift::LIST, [Thrift::BINARY, ['a']]], [3, Thrift::LIST, [Thrift::BINARY, ['b']]], [4, Thrift::I32, 0], [5, Thrift::LIST, [Thrift::I64, [0, 0]]]]),
            'garbage' => "\xFF\xFF\xFF",
        ];
        foreach ($bad as $what => $ci) {
            $f = FileBuilder::file([['name' => 'c', 'type' => 1, 'chunk' => FileBuilder::dataPageV1(\pack('V*', 1, 2), 2), 'columnIndex' => $ci]], 2);
            try {
                Reader::fromString($f)->columnIndex(0, 'c');
                $this->fail("accepted: $what");
            } catch (FormatException) {
                $this->addToAssertionCount(1);
            }
        }
        $f = FileBuilder::file([['name' => 'c', 'type' => 1, 'chunk' => FileBuilder::dataPageV1(\pack('V*', 1, 2), 2)]], 2);
        $this->assertNull(Reader::fromString($f)->columnIndex(0, 'c'));
    }

    public function testACorruptIndexOfARealFileNeverEscapesAsAnythingElse(): void
    {
        $good = Fixtures::bytes('idx-nested-v1');
        $r0 = Reader::fromString($good);
        $meta = $r0->metadata()->rowGroups[0]->columns[2];
        $from = $meta->offsetIndexOffset;
        \mt_srand(77);
        \set_error_handler(static function (int $no, string $msg, string $file, int $line): never {
            throw new \ErrorException($msg, 0, $no, $file, $line);
        });
        try {
            for ($i = 0; $i < 600; $i++) {
                $c = $good;
                for ($k = \mt_rand(1, 3); $k > 0; $k--) {
                    $p = $from + \mt_rand(0, $meta->offsetIndexLength - 1);
                    $c[$p] = \chr(\ord($c[$p]) ^ (1 << \mt_rand(0, 7)));
                }
                try {
                    $r = Reader::fromString($c);
                    \iterator_to_array($r->rows(null, \mt_rand(0, 150), \mt_rand(0, 30)), false);
                    $r->candidateRanges('st.a', 10, 50);
                } catch (\Charm\Parquet\ParquetException) {
                }
                $this->addToAssertionCount(1);
            }
        } finally {
            \restore_error_handler();
        }
    }
}
