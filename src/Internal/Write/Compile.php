<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

use Charm\Parquet\{ConvertedType, Type};
use Charm\Parquet\Thrift\CompactWriter as T;

/** Types to the schema being written: levels, leaves, and the SchemaElements of the footer in the standard modern forms. */
final class Compile
{
    /**
     * @param array<string, Type> $fields
     * @return array{list<WNode>, list<WNode>} the top-level nodes and all leaves in schema order
     */
    public static function fields(array $fields): array
    {
        if ($fields === []) {
            throw new \ValueError('A schema needs at least one column');
        }
        $leaves = [];
        $top = [];
        foreach ($fields as $name => $type) {
            if (!$type instanceof Type) {
                throw new \TypeError("Column '$name' is not a Type");
            }
            $name = (string) $name;
            if ($name === '') {
                throw new \ValueError('A column name must not be empty');
            }
            $top[] = self::node($name, $type, $name, [$name], 0, 0, $leaves);
        }
        return [$top, $leaves];
    }

    /** @param list<WNode> $leaves */
    private static function node(string $name, Type $t, string $path, array $parts, int $defIn, int $repIn, array &$leaves): WNode
    {
        $n = new WNode($name, $t->kind, $t->nullable, $path, $parts);
        $n->maxDef = $defIn + ($t->nullable ? 1 : 0);
        $n->maxRep = $repIn;
        switch ($t->kind) {
            case 'struct':
                foreach ($t->children as $i => $c) {
                    $n->children[] = self::node($t->names[$i], $c, $path . '.' . $t->names[$i], [...$parts, $t->names[$i]], $n->maxDef, $repIn, $leaves);
                    $n->names[] = $t->names[$i];
                }
                break;
            case 'list':
                $n->repDef = $n->maxDef + 1;
                $n->repLevel = $repIn + 1;
                $n->children[] = self::node('element', $t->children[0], $path . '.list.element', [...$parts, 'list', 'element'], $n->repDef, $n->repLevel, $leaves);
                break;
            case 'map':
                $n->repDef = $n->maxDef + 1;
                $n->repLevel = $repIn + 1;
                $n->children[] = self::node('key', $t->children[0], $path . '.key_value.key', [...$parts, 'key_value', 'key'], $n->repDef, $n->repLevel, $leaves);
                $n->children[] = self::node('value', $t->children[1], $path . '.key_value.value', [...$parts, 'key_value', 'value'], $n->repDef, $n->repLevel, $leaves);
                break;
            default:
                $n->spec = LeafSpec::forType($t);
                $n->leafIndex = \count($leaves);
                $leaves[] = $n;
                $n->leaves = [$n];
                return $n;
        }
        foreach ($n->children as $c) {
            foreach ($c->leaves as $l) {
                $n->leaves[] = $l;
            }
        }
        return $n;
    }

    /**
     * The SchemaElements (a list of field lists for the Thrift writer) of the nodes, depth first.
     *
     * @param list<WNode> $top
     * @return list<list<array{int, int, mixed}>>
     */
    public static function elements(array $top): array
    {
        $out = [[[4, T::BINARY, 'schema'], [5, T::I32, \count($top)]]];
        foreach ($top as $n) {
            self::emit($n, $out);
        }
        return $out;
    }

    /** @param list<list<array{int, int, mixed}>> $out */
    private static function emit(WNode $n, array &$out): void
    {
        $rep = $n->nullable ? 1 : 0;
        switch ($n->kind) {
            case 'struct':
                $out[] = [[3, T::I32, $rep], [4, T::BINARY, $n->name], [5, T::I32, \count($n->children)]];
                foreach ($n->children as $c) {
                    self::emit($c, $out);
                }
                return;
            case 'list':
                $out[] = [[3, T::I32, $rep], [4, T::BINARY, $n->name], [5, T::I32, 1], [6, T::I32, ConvertedType::List->value], [10, T::STRUCT, [[3, T::STRUCT, []]]]];
                $out[] = [[3, T::I32, 2], [4, T::BINARY, 'list'], [5, T::I32, 1]];
                self::emit($n->children[0], $out);
                return;
            case 'map':
                $out[] = [[3, T::I32, $rep], [4, T::BINARY, $n->name], [5, T::I32, 1], [6, T::I32, ConvertedType::Map->value], [10, T::STRUCT, [[2, T::STRUCT, []]]]];
                $out[] = [[3, T::I32, 2], [4, T::BINARY, 'key_value'], [5, T::I32, 2]];
                self::emit($n->children[0], $out);
                self::emit($n->children[1], $out);
                return;
        }
        $s = $n->spec;
        $el = [[1, T::I32, $s->phys->value]];
        if ($s->length !== null) {
            $el[] = [2, T::I32, $s->length];
        }
        $el[] = [3, T::I32, $rep];
        $el[] = [4, T::BINARY, $n->name];
        if ($s->converted !== null) {
            $el[] = [6, T::I32, $s->converted];
        }
        if ($s->scale !== null) {
            $el[] = [7, T::I32, $s->scale];
            $el[] = [8, T::I32, $s->precision];
        }
        if ($s->logical !== null) {
            $el[] = [10, T::STRUCT, $s->logical];
        }
        $out[] = $el;
    }
}
