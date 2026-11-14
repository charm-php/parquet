<?php

declare(strict_types=1);

namespace Charm\Parquet;

/**
 * One column chunk in its shredded (Dremel) form: the non-null values densely, and for every entry the definition and
 * repetition level. A level list is null when the schema has no such level for the column (maximum 0). The number of entries is
 * the chunk's value count; the number of entries with repetition level 0 is the number of rows (every entry is a row when there is
 * no repetition). Values are in the Reader's value mode.
 */
final readonly class ShreddedColumn
{
    public function __construct(
        public SchemaNode $leaf,
        /** @var list<bool|int|float|string|array> the values of the entries whose definition level equals the maximum, in order */
        public array $values,
        /** @var list<int>|null */
        public ?array $definitionLevels,
        /** @var list<int>|null */
        public ?array $repetitionLevels,
    ) {
    }
}
