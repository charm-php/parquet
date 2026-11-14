<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\SchemaNode;

/** One leaf's shredded values with its slots and counts (see Assembler), while a top-level field is being assembled. */
final class LeafData
{
    /** @var list<mixed>|null the leaf's entries at its slot level, filled on first use */
    public ?array $column = null;

    /**
     * @param list<mixed> $values
     * @param array<int, list<int>> $slot
     * @param array<int, list<int>> $cnt
     */
    public function __construct(
        public readonly SchemaNode $leaf,
        public readonly array $values,
        public readonly array $slot,
        public readonly array $cnt,
        public readonly int $m,
    ) {
    }
}
