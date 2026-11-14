<?php

declare(strict_types=1);

namespace Charm\Parquet;

/** One node of the schema tree: a group (has children) or a leaf (a column with a physical type). The root is a group without repetition. */
final readonly class SchemaNode
{
    public function __construct(
        public string $name,
        /** null for the root */
        public ?Repetition $repetition,
        /** null for groups */
        public ?PhysicalType $physicalType,
        public ?int $typeLength,
        public ?ConvertedType $convertedType,
        public ?LogicalType $logicalType,
        public ?int $fieldId,
        /** Legacy decimal parameters of the SchemaElement (the logical type carries them too) */
        public ?int $scale,
        public ?int $precision,
        /** @var list<SchemaNode> */
        public array $children,
        /** Names from below the root down to this node */
        public array $path,
        /** The path joined with '.'; '' for the root */
        public string $dottedPath,
        public int $maxDefinitionLevel,
        public int $maxRepetitionLevel,
        /** Position among the leaves in schema order (the order of the column chunks); null for groups */
        public ?int $leafIndex,
        /** The raw converted type id (set even when this version does not know it, then $convertedType is null) */
        public ?int $convertedTypeId = null,
    ) {
    }

    public function isLeaf(): bool
    {
        return $this->physicalType !== null;
    }

    /** True when every value is a single stored value or null: no repetition and at most one level of optionality. */
    public function isFlat(): bool
    {
        return $this->maxRepetitionLevel === 0 && $this->maxDefinitionLevel <= 1;
    }
}
