<?php

declare(strict_types=1);

namespace Charm\Parquet;

/** The logical type annotation of a schema element, with its parameters (those that do not apply to the kind are null). */
final readonly class LogicalType
{
    public function __construct(
        public LogicalKind $kind,
        /** Decimal */
        public ?int $precision = null,
        public ?int $scale = null,
        /** Time, Timestamp */
        public ?bool $isAdjustedToUtc = null,
        public ?TimeUnit $unit = null,
        /** Integer */
        public ?int $bitWidth = null,
        public ?bool $isSigned = null,
        /** Variant: specification version; Geometry, Geography: the CRS string; Geography: the algorithm number */
        public ?int $specificationVersion = null,
        public ?string $crs = null,
        public ?int $algorithm = null,
    ) {
    }
}
