<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Codec, Reader, TimeUnit, Type, Writer};
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

/** The writer holds one row group, not the file: peak memory is bounded by the row group limits, whatever the row count. */
final class WriterMemoryTest extends TestCase
{
    /**
     * 2 million rows of 5 columns with row groups of 50,000 rows. Measured: a buffered row of this schema costs about 230 bytes of PHP
     * memory (the writer's own estimate is 185), and flushing a group needs about 1.5 times what the group holds. The bound is derived
     * from the limit, not from a measurement of this run: rows per group * 5 columns * 64 bytes (the largest per-cell cost seen, a
     * short string) * 2.5 (the flush), plus 16 MB for code and the output string of one group.
     */
    public function testTwoMillionRowsStayWithinTheRowGroupBound(): void
    {
        $groupRows = 50000;
        $columns = 5;
        $bound = $groupRows * $columns * 64 * 5 / 2 + (16 << 20);
        $w = new Writer(
            ['id' => Type::int64()->required(), 'name' => Type::string(), 'score' => Type::double(), 'ts' => Type::timestamp(TimeUnit::Micros, true), 'flag' => Type::boolean()],
            codec: Codec::Uncompressed,
            rowGroupRows: $groupRows,
        );
        \gc_collect_cycles();
        $before = \memory_get_usage();
        \memory_reset_peak_usage();
        $written = 0;
        $groups = 0;
        for ($i = 0; $i < 2000000; $i++) {
            $out = $w->encode(['id' => $i, 'name' => 'user-' . ($i % 5000), 'score' => $i / 7, 'ts' => 1700000000000000 + $i * 1000, 'flag' => $i % 2 === 0]);
            if ($out !== '') {
                $written += \strlen($out);
                ++$groups;
            }
        }
        $tail = $w->finish();
        $written += \strlen($tail);
        $peak = \memory_get_peak_usage() - $before;
        $this->assertSame(41, $groups, 'outputs while encoding: the magic with the first row, then 40 row groups of 50,000 rows');
        $this->assertGreaterThan(10 << 20, $written);
        $this->assertLessThan($bound, $peak, \sprintf('peak %.1f MB over the start, bound %.1f MB', $peak / 1e6, $bound / 1e6));
        $this->assertSame(2000000, $w->rowCount());
    }
}
