<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\{ConvertedType, LogicalKind, MapMode, PhysicalType, Repetition, SchemaNode, ValueMode};

/**
 * What a schema node turns into, as PHP values: a leaf (a value), a struct (an associative array), a list, or a map. Built from the
 * schema (and the set of selected leaves) once per column read; it carries everything the Assembler needs to know about the schema,
 * so that no schema rule is looked up per value.
 *
 * The rules (Parquet format spec, "Nested Types" and "Backward-compatibility rules"):
 *   LIST annotation, repeated child R, element = the child of R (3-level form `list { element }`), except
 *     R is a repeated primitive: the list of R's values (1-level form inside a LIST group)
 *     R is itself annotated LIST or MAP: R is the element, a list or map (old-style list of lists)
 *     R is a group with more than one child, or one child and R is named "array" or "<list>_tuple": R is the element struct (2-level form)
 *   MAP (or MAP_KEY_VALUE) annotation: its repeated group child holds key (first) and value (second, may be missing: a set)
 *   a repeated field outside of those: a list of its values (a primitive) or of structs (a group)
 *   any other group: a struct
 */
final class Shape
{
    public const LEAF = 0;
    public const STRUCT = 1;
    public const LIST = 2;
    public const MAP = 3;

    /** @var array<string, Shape> struct fields */
    public array $fields = [];
    /** A struct is null where the definition level of its slot is below this (null: it cannot be null itself) */
    public ?int $threshold = null;
    /** List and map: the repetition level of the repeated node (its elements live at this slot level) */
    public int $level = 0;
    public ?Shape $element = null;
    public ?Shape $key = null;
    public ?Shape $value = null;
    public bool $assoc = false;
    /** @var list<SchemaNode> the selected leaves below, the first one drives this node's structure */
    public array $leaves = [];

    private function __construct(public readonly int $kind, public readonly ?SchemaNode $leaf = null)
    {
    }

    /** The shape of the value of $n, the field itself (a repeated field becomes a list), or null when no selected leaf is below it. */
    public static function field(SchemaNode $n, ?array $sel, MapMode $mapMode, ValueMode $valueMode): ?Shape
    {
        if (!self::any($n, $sel)) {
            return null;
        }
        if ($n->repetition === Repetition::Repeated) {
            $el = $n->isLeaf() ? self::leaf($n) : self::struct($n, $sel, $mapMode, $valueMode, false);
            return self::listOf($n->maxRepetitionLevel, $el);
        }
        return self::value($n, $sel, $mapMode, $valueMode);
    }

    /** True when a selected leaf lies at or below $n. */
    private static function any(SchemaNode $n, ?array $sel): bool
    {
        if ($n->isLeaf()) {
            return $sel === null || isset($sel[$n->leafIndex]);
        }
        foreach ($n->children as $c) {
            if (self::any($c, $sel)) {
                return true;
            }
        }
        return false;
    }

    private static function leaf(SchemaNode $n): Shape
    {
        $s = new Shape(self::LEAF, $n);
        $s->leaves = [$n];
        return $s;
    }

    private static function listOf(int $level, Shape $element): Shape
    {
        $s = new Shape(self::LIST);
        $s->level = $level;
        $s->element = $element;
        $s->leaves = $element->leaves;
        return $s;
    }

    private static function value(SchemaNode $n, ?array $sel, MapMode $mm, ValueMode $vm): Shape
    {
        if ($n->isLeaf()) {
            return self::leaf($n);
        }
        $kind = self::annotation($n);
        $r = \count($n->children) === 1 ? $n->children[0] : null;
        if ($kind === 'list' && $r !== null && $r->repetition === Repetition::Repeated) {
            return self::annotatedList($n, $r, $sel, $mm, $vm);
        }
        if ($kind === 'map' && $r !== null && $r->repetition === Repetition::Repeated && !$r->isLeaf() && \count($r->children) >= 1 && \count($r->children) <= 2) {
            return self::map($r, $sel, $mm, $vm);
        }
        return self::struct($n, $sel, $mm, $vm, true);
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

    private static function annotatedList(SchemaNode $list, SchemaNode $r, ?array $sel, MapMode $mm, ValueMode $vm): Shape
    {
        if ($r->isLeaf()) {
            $el = self::leaf($r);
        } elseif (self::annotation($r) !== null) {
            // the repeated group is itself a LIST or MAP (a list of lists, old style): it is the element, and its own annotation says what it is
            $el = self::value($r, $sel, $mm, $vm);
        } elseif (\count($r->children) > 1 || $r->name === 'array' || $r->name === $list->name . '_tuple') {
            $el = self::struct($r, $sel, $mm, $vm, false);
        } else {
            $el = self::field($r->children[0], $sel, $mm, $vm);
        }
        return self::listOf($r->maxRepetitionLevel, $el ?? self::struct($r, $sel, $mm, $vm, false));
    }

    private static function map(SchemaNode $r, ?array $sel, MapMode $mm, ValueMode $vm): Shape
    {
        $key = self::field($r->children[0], $sel, $mm, $vm);
        if ($key === null) {
            // only the value is selected: the entries as a list of structs holding what is selected
            return self::listOf($r->maxRepetitionLevel, self::struct($r, $sel, $mm, $vm, false));
        }
        $value = isset($r->children[1]) ? self::field($r->children[1], $sel, $mm, $vm) : null;
        $s = new Shape(self::MAP);
        $s->level = $r->maxRepetitionLevel;
        $s->key = $key;
        $s->value = $value;
        $s->leaves = \array_merge($key->leaves, $value->leaves ?? []);
        $s->assoc = $mm === MapMode::Auto && $key->kind === self::LEAF && self::keyIsScalar($key->leaf, $vm);
        return $s;
    }

    /** Is the PHP value of this key column always an int or a string (a usable array key)? Decided by the schema alone. */
    private static function keyIsScalar(SchemaNode $leaf, ValueMode $vm): bool
    {
        $p = $leaf->physicalType;
        if ($p === PhysicalType::FixedLenByteArray) {
            if ($vm === ValueMode::Logical && (($leaf->logicalType?->kind === LogicalKind::Float16 && $leaf->typeLength === 2)
                || ($leaf->convertedType === ConvertedType::Interval && $leaf->typeLength === 12))) {
                return false;
            }
            return true;
        }
        return $p === PhysicalType::Int32 || $p === PhysicalType::Int64 || $p === PhysicalType::ByteArray;
    }

    private static function struct(SchemaNode $n, ?array $sel, MapMode $mm, ValueMode $vm, bool $nullable): Shape
    {
        $s = new Shape(self::STRUCT);
        foreach ($n->children as $c) {
            $f = self::field($c, $sel, $mm, $vm);
            if ($f !== null) {
                $s->fields[$c->name] = $f;
                $s->leaves = \array_merge($s->leaves, $f->leaves);
            }
        }
        $s->threshold = $nullable && $n->repetition === Repetition::Optional ? $n->maxDefinitionLevel : null;
        return $s;
    }
}
