<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\{ColumnChunk, Codec, ConvertedType, Encoding, FileMetadata, FormatException, LogicalKind, LogicalType, PhysicalType, Repetition};
use Charm\Parquet\{RowGroup, Schema, SchemaNode, Statistics, TimeUnit, UnsupportedException};
use Charm\Parquet\Thrift\CompactReader;

/** Turns the footer (a Thrift FileMetaData) into the metadata model, validating what the rest of the reader relies on. */
final class MetadataParser
{
    private const MAX_SCHEMA_DEPTH = 64;

    public static function parse(string $footer): FileMetadata
    {
        $r = new CompactReader($footer);
        $s = $r->readStruct();
        $version = Fields::int($s, 1, 'FileMetaData.version');
        $elements = Fields::list($s, 2, 'FileMetaData.schema');
        $numRows = Fields::int($s, 3, 'FileMetaData.num_rows');
        if ($numRows < 0) {
            throw new FormatException('FileMetaData.num_rows is negative');
        }
        $schema = self::schema($elements);
        $groups = [];
        foreach (Fields::optList($s, 4, 'FileMetaData.row_groups') as $i => $rg) {
            if (!\is_array($rg)) {
                throw new FormatException("row group $i is not a struct");
            }
            $groups[] = self::rowGroup($rg, $schema, $i);
        }
        return new FileMetadata(
            $version,
            $schema,
            $numRows,
            $groups,
            self::keyValues(Fields::optList($s, 5, 'FileMetaData.key_value_metadata')),
            Fields::optString($s, 6, 'FileMetaData.created_by'),
        );
    }

    /** @param list<mixed> $elements */
    private static function schema(array $elements): Schema
    {
        $n = \count($elements);
        if ($n === 0) {
            throw new FormatException('The schema has no elements');
        }
        $i = 0;
        $leaf = 0;
        $root = self::node($elements, $i, $n, [], 0, 0, true, 0, $leaf);
        if ($i !== $n) {
            throw new FormatException('The schema has ' . ($n - $i) . ' elements that belong to no group');
        }
        if ($root->isLeaf()) {
            throw new FormatException('The schema root is not a group');
        }
        return new Schema($root);
    }

    /**
     * @param list<mixed> $el
     * @param list<string> $parentPath
     */
    private static function node(array $el, int &$i, int $n, array $parentPath, int $defLevel, int $repLevel, bool $isRoot, int $depth, int &$leaf): SchemaNode
    {
        if ($i >= $n) {
            throw new FormatException('The schema ends before all groups are complete');
        }
        if ($depth > self::MAX_SCHEMA_DEPTH) {
            throw new FormatException('The schema is nested deeper than ' . self::MAX_SCHEMA_DEPTH . ' levels');
        }
        $e = $el[$i++];
        if (!\is_array($e)) {
            throw new FormatException('A schema element is not a struct');
        }
        $name = Fields::optString($e, 4, 'SchemaElement.name');
        if ($name === null) {
            throw new FormatException('A schema element has no name');
        }
        $type = Fields::optInt($e, 1, 'SchemaElement.type');
        $physical = $type === null ? null : (PhysicalType::tryFrom($type) ?? throw new FormatException("Unknown physical type $type"));
        $rep = Fields::optInt($e, 3, 'SchemaElement.repetition_type');
        $repetition = $isRoot ? null : ($rep === null ? Repetition::Required : (Repetition::tryFrom($rep) ?? throw new FormatException("Unknown repetition type $rep")));
        $numChildren = Fields::optInt($e, 5, 'SchemaElement.num_children');
        $length = Fields::optInt($e, 2, 'SchemaElement.type_length');
        $ct = Fields::optInt($e, 6, 'SchemaElement.converted_type');
        $converted = $ct === null ? null : ConvertedType::tryFrom($ct); // (an id this version does not know stays in convertedTypeId)
        $lt = Fields::optStruct($e, 10, 'SchemaElement.logicalType');

        $path = $isRoot ? [] : [...$parentPath, $name];
        $def = $defLevel + ($repetition === Repetition::Optional || $repetition === Repetition::Repeated ? 1 : 0);
        $rl = $repLevel + ($repetition === Repetition::Repeated ? 1 : 0);

        $children = [];
        $isGroup = $physical === null;
        if ($isGroup) {
            if ($numChildren === null || $numChildren < 0) {
                throw new FormatException("Schema element '$name' has no type and no children");
            }
            if ($numChildren > $n - $i) {
                throw new FormatException("Schema group '$name' announces $numChildren children, but only " . ($n - $i) . ' elements are left');
            }
            for ($c = 0; $c < $numChildren; $c++) {
                $children[] = self::node($el, $i, $n, $path, $def, $rl, false, $depth + 1, $leaf);
            }
        } elseif ($numChildren !== null && $numChildren > 0) {
            throw new FormatException("Schema element '$name' has a type and children");
        }
        if ($physical === PhysicalType::FixedLenByteArray && ($length === null || $length <= 0)) {
            throw new FormatException("Schema element '$name' is FIXED_LEN_BYTE_ARRAY without a positive type_length");
        }
        return new SchemaNode(
            $name,
            $repetition,
            $physical,
            $length,
            $converted,
            $lt === null ? null : self::logicalType($lt),
            Fields::optInt($e, 9, 'SchemaElement.field_id'),
            Fields::optInt($e, 7, 'SchemaElement.scale'),
            Fields::optInt($e, 8, 'SchemaElement.precision'),
            $children,
            $path,
            \implode('.', $path),
            $def,
            $rl,
            $isGroup ? null : $leaf++,
            $ct,
        );
    }

    /** @param array<int, mixed> $u the LogicalType union */
    private static function logicalType(array $u): LogicalType
    {
        foreach ($u as $id => $v) {
            $kind = LogicalKind::tryFrom($id) ?? LogicalKind::Unrecognized;
            $p = \is_array($v) ? $v : [];
            switch ($kind) {
                case LogicalKind::Decimal:
                    return new LogicalType($kind, precision: Fields::optInt($p, 2, 'DecimalType.precision'), scale: Fields::optInt($p, 1, 'DecimalType.scale'));
                case LogicalKind::Time:
                case LogicalKind::Timestamp:
                    $unit = null;
                    $us = Fields::optStruct($p, 2, 'unit');
                    if ($us !== null) {
                        foreach ($us as $uid => $_) {
                            $unit = TimeUnit::tryFrom($uid);
                        }
                    }
                    return new LogicalType($kind, isAdjustedToUtc: Fields::optBool($p, 1, 'isAdjustedToUTC'), unit: $unit);
                case LogicalKind::Integer:
                    return new LogicalType($kind, bitWidth: Fields::optInt($p, 1, 'IntType.bitWidth'), isSigned: Fields::optBool($p, 2, 'IntType.isSigned'));
                case LogicalKind::Variant:
                    return new LogicalType($kind, specificationVersion: Fields::optInt($p, 1, 'VariantType.specification_version'));
                case LogicalKind::Geometry:
                    return new LogicalType($kind, crs: Fields::optString($p, 1, 'GeometryType.crs'));
                case LogicalKind::Geography:
                    return new LogicalType($kind, crs: Fields::optString($p, 1, 'GeographyType.crs'), algorithm: Fields::optInt($p, 2, 'GeographyType.algorithm'));
                default:
                    return new LogicalType($kind);
            }
        }
        return new LogicalType(LogicalKind::Unrecognized);
    }

    /** @param array<int, mixed> $rg */
    private static function rowGroup(array $rg, Schema $schema, int $index): RowGroup
    {
        $cols = Fields::list($rg, 1, "RowGroup $index columns");
        $leaves = $schema->leaves;
        if (\count($cols) !== \count($leaves)) {
            throw new FormatException("Row group $index has " . \count($cols) . ' column chunks for ' . \count($leaves) . ' columns');
        }
        $numRows = Fields::int($rg, 3, "RowGroup $index num_rows");
        if ($numRows < 0) {
            throw new FormatException("Row group $index has a negative row count");
        }
        $chunks = [];
        foreach ($cols as $k => $c) {
            if (!\is_array($c)) {
                throw new FormatException("A column chunk of row group $index is not a struct");
            }
            $chunks[] = self::columnChunk($c, $leaves[$k], $index);
        }
        return new RowGroup(
            $chunks,
            Fields::optInt($rg, 2, "RowGroup $index total_byte_size") ?? 0,
            $numRows,
            Fields::optInt($rg, 5, "RowGroup $index file_offset"),
            Fields::optInt($rg, 6, "RowGroup $index total_compressed_size"),
            Fields::optInt($rg, 7, "RowGroup $index ordinal"),
        );
    }

    /** @param array<int, mixed> $c */
    private static function columnChunk(array $c, SchemaNode $leaf, int $rgIndex): ColumnChunk
    {
        $what = "Column '{$leaf->dottedPath}' of row group $rgIndex";
        $md = Fields::optStruct($c, 3, "$what meta_data");
        $encrypted = isset($c[8]);
        if ($md === null) {
            // (encrypted column metadata, or none at all): only this column cannot be read
            return new ColumnChunk($leaf, $leaf->physicalType, [], null, 0, 0, 0, 0, null, null, null, [], Fields::optString($c, 1, "$what file_path"),
                Fields::optInt($c, 2, "$what file_offset") ?? 0, null, null, null, null, null, null, $encrypted, 0, [],
                isset($c[9]) || $encrypted ? "encrypted column metadata in column '{$leaf->dottedPath}'" : "column '{$leaf->dottedPath}' has no column metadata");
        }
        $t = Fields::int($md, 1, "$what type");
        $type = PhysicalType::tryFrom($t) ?? throw new FormatException("$what has unknown physical type $t");
        if ($type !== $leaf->physicalType) {
            throw new FormatException("$what has type {$type->name}, the schema says {$leaf->physicalType?->name}");
        }
        $path = [];
        foreach (Fields::list($md, 3, "$what path_in_schema") as $part) {
            if (!\is_string($part)) {
                throw new FormatException("$what has a path_in_schema entry that is not a string");
            }
            $path[] = $part;
        }
        if ($path !== $leaf->path) {
            throw new FormatException("$what: path_in_schema '" . \implode('.', $path) . "' does not match the schema");
        }
        $encodings = [];
        $encodingIds = [];
        foreach (Fields::list($md, 2, "$what encodings") as $e) {
            if (!\is_int($e)) {
                throw new FormatException("$what has an encoding that is not an integer");
            }
            $encodingIds[] = $e;
            $known = Encoding::tryFrom($e);
            if ($known !== null) {
                $encodings[] = $known;
            }
        }
        $codecId = Fields::int($md, 4, "$what codec");
        $codec = Codec::tryFrom($codecId);
        $numValues = Fields::int($md, 5, "$what num_values");
        $uncompressed = Fields::int($md, 6, "$what total_uncompressed_size");
        $compressed = Fields::int($md, 7, "$what total_compressed_size");
        $dataOffset = Fields::int($md, 9, "$what data_page_offset");
        if ($numValues < 0 || $uncompressed < 0 || $compressed < 0 || $dataOffset < 0) {
            throw new FormatException("$what has a negative size, count or offset");
        }
        $stats = Fields::optStruct($md, 12, "$what statistics");
        return new ColumnChunk(
            $leaf,
            $type,
            $encodings,
            $codec,
            $numValues,
            $uncompressed,
            $compressed,
            $dataOffset,
            Fields::optInt($md, 10, "$what index_page_offset"),
            Fields::optInt($md, 11, "$what dictionary_page_offset"),
            $stats === null ? null : self::statistics($stats),
            self::keyValues(Fields::optList($md, 8, "$what key_value_metadata")),
            Fields::optString($c, 1, "$what file_path"),
            Fields::optInt($c, 2, "$what file_offset") ?? 0,
            Fields::optInt($c, 4, "$what offset_index_offset"),
            Fields::optInt($c, 5, "$what offset_index_length"),
            Fields::optInt($c, 6, "$what column_index_offset"),
            Fields::optInt($c, 7, "$what column_index_length"),
            Fields::lenientInt($md, 14),
            Fields::lenientInt($md, 15),
            $encrypted,
            $codecId,
            $encodingIds,
            $codec === null ? "codec $codecId" : null,
        );
    }

    /** @param array<int, mixed> $s */
    public static function statistics(array $s): Statistics
    {
        return new Statistics(
            Fields::optString($s, 1, 'Statistics.max'),
            Fields::optString($s, 2, 'Statistics.min'),
            Fields::optInt($s, 3, 'Statistics.null_count'),
            Fields::optInt($s, 4, 'Statistics.distinct_count'),
            Fields::optString($s, 5, 'Statistics.max_value'),
            Fields::optString($s, 6, 'Statistics.min_value'),
            Fields::optBool($s, 7, 'Statistics.is_max_value_exact'),
            Fields::optBool($s, 8, 'Statistics.is_min_value_exact'),
        );
    }

    /**
     * @param list<mixed> $list
     * @return array<string, ?string>
     */
    private static function keyValues(array $list): array
    {
        $out = [];
        foreach ($list as $kv) {
            if (!\is_array($kv)) {
                throw new FormatException('A key-value entry is not a struct');
            }
            $out[Fields::optString($kv, 1, 'KeyValue.key') ?? throw new FormatException('A key-value entry has no key')] = Fields::optString($kv, 2, 'KeyValue.value');
        }
        return $out;
    }
}
