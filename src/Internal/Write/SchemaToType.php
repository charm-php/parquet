<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

use Charm\Parquet\{ConvertedType, LogicalKind, PhysicalType, Repetition, Schema, SchemaNode, TimeUnit, Type, UnsupportedException};

/**
 * The schema of a file read with Reader as Types, so that a file can be rewritten with the schema it was read with. Legacy list and map
 * forms become standard ones (the rules are those of Internal\Shape); what cannot be written throws UnsupportedException naming it.
 */
final class SchemaToType
{
    /** @return array<string, Type> */
    public static function fields(Schema $schema): array
    {
        $out = [];
        foreach ($schema->root->children as $c) {
            $out[$c->name] = self::field($c);
        }
        return $out;
    }

    /** The Type of a field, a repeated one being a (required) list. */
    private static function field(SchemaNode $n): Type
    {
        if ($n->repetition === Repetition::Repeated) {
            return Type::list(self::element($n))->required();
        }
        $t = self::value($n);
        return $n->repetition === Repetition::Required ? $t->required() : $t;
    }

    /** The Type of the elements of a repeated node (its values, never null). */
    private static function element(SchemaNode $r): Type
    {
        return ($r->isLeaf() ? self::leaf($r) : self::struct($r))->required();
    }

    private static function annotation(SchemaNode $n): ?string
    {
        $k = $n->logicalType?->kind;
        if ($k === LogicalKind::List) {
            return 'list';
        }
        if ($k === LogicalKind::Map) {
            return 'map';
        }
        return match ($n->convertedType) {
            ConvertedType::List => 'list',
            ConvertedType::Map, ConvertedType::MapKeyValue => 'map',
            default => null,
        };
    }

    private static function value(SchemaNode $n): Type
    {
        if ($n->isLeaf()) {
            return self::leaf($n);
        }
        $kind = self::annotation($n);
        $r = \count($n->children) === 1 ? $n->children[0] : null;
        if ($kind === 'list' && $r !== null && $r->repetition === Repetition::Repeated) {
            if ($r->isLeaf()) {
                $el = self::leaf($r)->required();
            } elseif (self::annotation($r) !== null) {
                $el = self::value($r)->required();
            } elseif (\count($r->children) > 1 || $r->name === 'array' || $r->name === $n->name . '_tuple') {
                $el = self::struct($r)->required();
            } else {
                $el = self::field($r->children[0]);
            }
            return Type::list($el);
        }
        if ($kind === 'map' && $r !== null && $r->repetition === Repetition::Repeated && !$r->isLeaf() && \count($r->children) >= 1 && \count($r->children) <= 2) {
            if (!isset($r->children[1])) {
                throw new UnsupportedException("MAP without a value column (a set): '{$n->dottedPath}'");
            }
            return Type::map(self::field($r->children[0]), self::field($r->children[1]));
        }
        return self::struct($n);
    }

    private static function struct(SchemaNode $n): Type
    {
        $f = [];
        foreach ($n->children as $c) {
            $f[$c->name] = self::field($c);
        }
        if ($f === []) {
            throw new UnsupportedException("group without fields: '{$n->dottedPath}'");
        }
        return Type::struct($f);
    }

    private static function leaf(SchemaNode $n): Type
    {
        $lt = $n->logicalType;
        $k = $lt !== null && $lt->kind !== LogicalKind::Unrecognized && $lt->kind !== LogicalKind::Unknown ? $lt->kind : null;
        $c = $n->convertedType;
        $path = $n->dottedPath;
        switch ($n->physicalType) {
            case PhysicalType::Boolean:
                return Type::boolean();
            case PhysicalType::Float:
                return Type::float();
            case PhysicalType::Double:
                return Type::double();
            case PhysicalType::Int96:
                throw new UnsupportedException("INT96 column '$path'");
            case PhysicalType::Int32:
            case PhysicalType::Int64:
                $is32 = $n->physicalType === PhysicalType::Int32;
                if ($k === LogicalKind::Integer || ($k === null && $c !== null && \in_array($c, [ConvertedType::Int8, ConvertedType::Int16, ConvertedType::Int32, ConvertedType::Int64, ConvertedType::Uint8, ConvertedType::Uint16, ConvertedType::Uint32, ConvertedType::Uint64], true))) {
                    [$bits, $signed] = $k === LogicalKind::Integer ? [$lt->bitWidth ?? 0, $lt->isSigned ?? true] : match ($c) {
                        ConvertedType::Int8 => [8, true], ConvertedType::Int16 => [16, true], ConvertedType::Int32 => [32, true], ConvertedType::Int64 => [64, true],
                        ConvertedType::Uint8 => [8, false], ConvertedType::Uint16 => [16, false], ConvertedType::Uint32 => [32, false], default => [64, false],
                    };
                    if (($is32 && $bits <= 32) || (!$is32 && $bits === 64)) {
                        return match (true) {
                            $signed && $bits === 8 => Type::int8(), $signed && $bits === 16 => Type::int16(), $signed && $bits === 32 => Type::int32(), $signed => Type::int64(),
                            $bits === 8 => Type::uint8(), $bits === 16 => Type::uint16(), $bits === 32 => Type::uint32(), default => Type::uint64(),
                        };
                    }
                }
                if (($k === LogicalKind::Decimal || ($k === null && $c === ConvertedType::Decimal))) {
                    return Type::decimal($lt?->precision ?? $n->precision ?? 0, $lt?->scale ?? $n->scale ?? 0);
                }
                if ($is32 && ($k === LogicalKind::Date || ($k === null && $c === ConvertedType::Date))) {
                    return Type::date();
                }
                if ($k === LogicalKind::Time && $lt->unit !== null && ($is32 === ($lt->unit === TimeUnit::Millis))) {
                    return Type::time($lt->unit, $lt->isAdjustedToUtc ?? false);
                }
                if ($k === null && $is32 && $c === ConvertedType::TimeMillis) {
                    return Type::time(TimeUnit::Millis);
                }
                if ($k === null && !$is32 && $c === ConvertedType::TimeMicros) {
                    return Type::time(TimeUnit::Micros);
                }
                if (!$is32 && $k === LogicalKind::Timestamp && $lt->unit !== null) {
                    return Type::timestamp($lt->unit, $lt->isAdjustedToUtc ?? false);
                }
                if (!$is32 && $k === null && $c === ConvertedType::TimestampMillis) {
                    return Type::timestamp(TimeUnit::Millis, true);
                }
                if (!$is32 && $k === null && $c === ConvertedType::TimestampMicros) {
                    return Type::timestamp(TimeUnit::Micros, true);
                }
                return $is32 ? Type::int32() : Type::int64();
            case PhysicalType::ByteArray:
                if ($k === LogicalKind::Enum || ($k === null && $c === ConvertedType::Enum)) {
                    throw new UnsupportedException("ENUM column '$path'");
                }
                if ($k === LogicalKind::Bson || ($k === null && $c === ConvertedType::Bson)) {
                    throw new UnsupportedException("BSON column '$path'");
                }
                if ($k === LogicalKind::Json || ($k === null && $c === ConvertedType::Json)) {
                    return Type::json();
                }
                if ($k === LogicalKind::Decimal || ($k === null && $c === ConvertedType::Decimal)) {
                    return Type::decimal($lt?->precision ?? $n->precision ?? 0, $lt?->scale ?? $n->scale ?? 0);
                }
                if ($k === LogicalKind::String || ($k === null && $c === ConvertedType::Utf8)) {
                    return Type::string();
                }
                return Type::bytes();
            default: // FIXED_LEN_BYTE_ARRAY
                $len = $n->typeLength ?? 0;
                if ($k === LogicalKind::Uuid && $len === 16) {
                    return Type::uuid();
                }
                if ($k === LogicalKind::Float16 && $len === 2) {
                    return Type::float16();
                }
                if ($k === LogicalKind::Decimal || ($k === null && $c === ConvertedType::Decimal)) {
                    return Type::decimal($lt?->precision ?? $n->precision ?? 0, $lt?->scale ?? $n->scale ?? 0);
                }
                if ($k === null && $c === ConvertedType::Interval) {
                    throw new UnsupportedException("INTERVAL column '$path'");
                }
                return Type::fixed($len);
        }
    }
}
