<?php

declare(strict_types=1);

namespace Charm\Parquet;

/** The pages of one column chunk (a data page starts a row; the dictionary page, if any, is not listed). */
final readonly class OffsetIndex
{
    public function __construct(
        /** @var list<PageLocation> */
        public array $pages,
    ) {
    }

    /**
     * The row after the last row that page $i starts: the first row of the next page that starts a later row, or the row group's row
     * count. (A page of a repeated column may continue the row the page before it started; pyarrow writes v1 pages that way and the
     * two then have the same first row index.)
     */
    public function endRow(int $i, int $rowGroupRows): int
    {
        for ($j = $i + 1, $n = \count($this->pages); $j < $n; $j++) {
            if ($this->pages[$j]->firstRowIndex > $this->pages[$i]->firstRowIndex) {
                return $this->pages[$j]->firstRowIndex;
            }
        }
        return $rowGroupRows;
    }
}
