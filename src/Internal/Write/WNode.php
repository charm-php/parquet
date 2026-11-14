<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

use Charm\Parquet\{PhysicalType, Type};

/** A node of the schema being written: the Type with the levels and, for leaves, the physical type and the value converter. */
final class WNode
{
    /** @var list<WNode> list: [element]; map: [key, value]; struct: the fields */
    public array $children = [];
    /** @var list<string> struct field names */
    public array $names = [];
    /** @var list<WNode> the leaves at or below this node, in schema order */
    public array $leaves = [];
    public ?int $leafIndex = null;
    public int $maxDef = 0;
    public int $maxRep = 0;
    /** list and map: definition and repetition level of the repeated group inside */
    public int $repDef = 0;
    public int $repLevel = 0;
    public ?LeafSpec $spec = null;

    /** @param list<string> $parts the names from the top-level column down: the path_in_schema */
    public function __construct(public readonly string $name, public readonly string $kind, public readonly bool $nullable, public readonly string $path, public readonly array $parts = [])
    {
    }

    public function isLeaf(): bool
    {
        return $this->spec !== null;
    }
}
