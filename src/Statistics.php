<?php

declare(strict_types=1);

namespace Charm\Parquet;

/** Column chunk statistics; min and max are the stored bytes, not decoded. `max`/`min` are the deprecated fields, `maxValue`/`minValue` the current ones. */
final readonly class Statistics
{
    public function __construct(
        public ?string $max = null,
        public ?string $min = null,
        public ?int $nullCount = null,
        public ?int $distinctCount = null,
        public ?string $maxValue = null,
        public ?string $minValue = null,
        public ?bool $isMaxValueExact = null,
        public ?bool $isMinValueExact = null,
    ) {
    }
}
