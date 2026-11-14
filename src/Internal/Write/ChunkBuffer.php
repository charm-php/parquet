<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

/** The entries of one leaf for the row group being written: the dense non-null values and the levels. */
final class ChunkBuffer
{
    /** @var list<mixed> */
    public array $vals = [];
    /** @var list<int> */
    public array $defs = [];
    /** @var list<int> */
    public array $reps = [];
    /** The PLAIN size of the variable-width values (4 + length each); fixed-width ones are counted when the chunk is encoded. */
    public int $plainBytes = 0;

    public function clear(): void
    {
        $this->vals = [];
        $this->defs = [];
        $this->reps = [];
        $this->plainBytes = 0;
    }
}
