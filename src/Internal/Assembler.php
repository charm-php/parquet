<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\{FormatException, SchemaNode, ShreddedColumn};

/**
 * Turns shredded columns (dense values plus definition and repetition levels) into PHP values, column by column.
 *
 * For one leaf, one pass over its levels yields, per list nesting level k (k = the repetition level of a repeated ancestor):
 *   $cnt[k]   one number per list instance at level k: the element count, -1 for a null list (an optional list or an ancestor is null),
 *             0 for an empty list; one instance per slot of level k-1
 *   $slot[j]  the definition level at which each slot of level j was created (level 0: the rows; level j: the elements of the lists of
 *             level j); the slot's own nulls (a null struct, a null value) are read from it
 * What a definition level means is decided by the thresholds of the schema: for list level k, "the list instance exists" is
 * def >= dR[k]-1 and "it has an element" is def >= dR[k] (dR[k] = the definition level of the repeated node); a struct is null where
 * the definition level of its slot is below its threshold; a value is present where the level equals the leaf's maximum.
 *
 * The values are then assembled bottom-up over the Shape (recursion over the schema, never over values): a leaf gives one entry per
 * slot of its level, a struct zips its fields' entries, and a list or map cuts the element entries into lists with array_slice().
 */
final class Assembler
{
    /**
     * The slots and counts of one leaf.
     *
     * @param list<int> $dR definition level of the repeated ancestor for list levels 1..m (index 0 is level 1)
     * @return array{array<int, list<int>>, array<int, list<int>>} [$slot by level 0..m, $cnt by level 1..m]
     */
    public static function slots(ShreddedColumn $c, array $dR): array
    {
        $m = $c->leaf->maxRepetitionLevel;
        $defs = $c->definitionLevels;
        if ($m === 0) {
            return [[0 => $defs ?? []], []];
        }
        $reps = $c->repetitionLevels ?? [];
        $n = \count($reps);
        if ($defs !== null && \count($defs) !== $n) {
            throw new FormatException("Column '{$c->leaf->dottedPath}' has " . \count($defs) . " definition levels and $n repetition levels");
        }
        if ($m === 1) {
            return self::slotsOneLevel($c, $dR[0], $reps, $defs);
        }
        $slot = \array_fill(0, $m + 1, []);
        $cnt = \array_fill(1, $m, []);
        $last = \array_fill(1, $m, -1); // index of the newest list instance per level
        $alive = 0;                      // how many list levels the previous entry left with an element
        $name = $c->leaf->dottedPath;
        for ($i = 0; $i < $n; $i++) {
            $r = $reps[$i];
            $d = $defs === null ? 0 : $defs[$i];
            if ($r === 0) {
                $slot[0][] = $d;
                $alive = 0;
            } else {
                if ($r > $alive || $d < $dR[$r - 1]) {
                    throw new FormatException("Column '$name': repetition level $r continues a list that has no element in progress");
                }
                $cnt[$r][$last[$r]]++;
                $slot[$r][] = $d;
                $alive = $r;
            }
            for ($k = $r + 1; $k <= $m; $k++) {
                if ($d >= $dR[$k - 1] - 1) {
                    if ($d >= $dR[$k - 1]) {
                        $cnt[$k][] = 1;
                        $slot[$k][] = $d;
                        $last[$k]++;
                        $alive = $k;
                        continue;
                    }
                    $cnt[$k][] = 0; // an empty list
                } else {
                    $cnt[$k][] = -1; // a null list (or an ancestor is null)
                }
                ++$last[$k];
                break;
            }
        }
        return [$slot, $cnt];
    }

    /**
     * slots() for a leaf with one list level (a list of values, or of structs of lists of nothing): the same logic without the arrays of arrays.
     *
     * @param list<int> $reps
     * @param list<int>|null $defs
     * @return array{array<int, list<int>>, array<int, list<int>>}
     */
    private static function slotsOneLevel(ShreddedColumn $c, int $dR, array $reps, ?array $defs): array
    {
        $n = \count($reps);
        $slot0 = [];
        $slot1 = [];
        $cnt = [];
        $ci = -1;       // index of the newest list instance in $cnt
        $alive = false; // the newest list has an element in progress
        $dG = $dR - 1;
        for ($i = 0; $i < $n; $i++) {
            $d = $defs === null ? 0 : $defs[$i];
            if ($reps[$i] === 0) {
                $slot0[] = $d;
                ++$ci;
                if ($d >= $dR) {
                    $cnt[] = 1;
                    $slot1[] = $d;
                    $alive = true;
                } else {
                    $cnt[] = $d >= $dG ? 0 : -1; // an empty list, or a null list (or an ancestor is null)
                    $alive = false;
                }
            } else {
                if (!$alive || $d < $dR) {
                    throw new FormatException("Column '{$c->leaf->dottedPath}': repetition level {$reps[$i]} continues a list that has no element in progress");
                }
                ++$cnt[$ci];
                $slot1[] = $d;
            }
        }
        return [[0 => $slot0, 1 => $slot1], [1 => $cnt]];
    }

    /**
     * The entries of a leaf at its own slot level: the value, or null where the definition level is below the maximum.
     *
     * @param list<mixed> $values
     * @param list<int> $slotDefs
     * @return list<mixed>
     */
    public static function leafColumn(SchemaNode $leaf, array $values, array $slotDefs, int $m): array
    {
        $max = $leaf->maxDefinitionLevel;
        if ($max === 0) {
            return $values;
        }
        $nSlots = \count($slotDefs);
        $nv = \count($values);
        if ($nv === $nSlots) {
            return $values; // (no nulls)
        }
        if ($nv === 0) {
            return \array_fill(0, $nSlots, null);
        }
        $out = [];
        $k = 0;
        foreach ($slotDefs as $d) {
            $out[] = $d === $max ? $values[$k++] : null;
        }
        if ($k !== $nv) {
            throw new FormatException("Column '{$leaf->dottedPath}': the definition levels place $k values, the page holds $nv");
        }
        return $out;
    }

    /**
     * @param array<int, LeafData> $data by leaf index
     * @return list<mixed> one entry per slot of level $j
     */
    public static function build(Shape $s, array $data, int $j): array
    {
        switch ($s->kind) {
            case Shape::LEAF:
                $d = $data[$s->leaf->leafIndex];
                return $d->column ??= self::leafColumn($s->leaf, $d->values, $d->slot[$d->m], $d->m);
            case Shape::STRUCT:
                return self::struct($s, $data, $j);
            default:
                return self::listOrMap($s, $data, $j);
        }
    }

    /** @param array<int, LeafData> $data */
    private static function struct(Shape $s, array $data, int $j): array
    {
        $names = \array_keys($s->fields);
        $cols = [];
        $n = null;
        foreach ($s->fields as $f) {
            $col = self::build($f, $data, $j);
            $cnt = \count($col);
            if ($n !== null && $cnt !== $n) {
                throw new FormatException('The columns of a struct disagree on how many values there are (' . $n . ' and ' . $cnt . ')');
            }
            $n = $cnt;
            $cols[] = $col;
        }
        $n ??= 0;
        $drv = $data[$s->leaves[0]->leafIndex];
        $defs = $drv->slot[$j] ?? [];
        if ($s->threshold !== null && \count($defs) !== $n) {
            throw new FormatException('A struct has ' . $n . ' values but ' . \count($defs) . ' definition levels');
        }
        $out = [];
        $nf = \count($names);
        $th = $s->threshold;
        for ($i = 0; $i < $n; $i++) {
            if ($th !== null && $defs[$i] < $th) {
                $out[] = null;
                continue;
            }
            $row = [];
            for ($f = 0; $f < $nf; $f++) {
                $row[$names[$f]] = $cols[$f][$i];
            }
            $out[] = $row;
        }
        return $out;
    }

    /** @param array<int, LeafData> $data */
    private static function listOrMap(Shape $s, array $data, int $j): array
    {
        $k = $j + 1;
        if ($s->level !== $k) {
            throw new FormatException("A list at repetition level {$s->level} sits at nesting level $j");
        }
        $drv = $data[$s->leaves[0]->leafIndex];
        $counts = $drv->cnt[$k] ?? [];
        foreach ($s->leaves as $l) {
            if (($data[$l->leafIndex]->cnt[$k] ?? []) !== $counts) {
                throw new FormatException("The columns below '{$s->leaves[0]->dottedPath}' disagree on the lengths of their lists");
            }
        }
        if ($s->kind === Shape::LIST) {
            $el = self::build($s->element, $data, $k);
            $pairs = null;
            $keys = null;
        } else {
            $keys = self::build($s->key, $data, $k);
            $vals = $s->value !== null ? self::build($s->value, $data, $k) : \array_fill(0, \count($keys), null);
            if (\count($keys) !== \count($vals)) {
                throw new FormatException('A map has ' . \count($keys) . ' keys and ' . \count($vals) . ' values');
            }
            $el = $keys;
        }
        $total = \count($el);
        $out = [];
        $off = 0;
        if ($s->kind === Shape::LIST) {
            foreach ($counts as $c) {
                if ($c < 0) {
                    $out[] = null;
                } elseif ($c === 0) {
                    $out[] = [];
                } else {
                    $out[] = \array_slice($el, $off, $c);
                    $off += $c;
                }
            }
        } elseif ($s->assoc) {
            foreach ($counts as $c) {
                if ($c < 0) {
                    $out[] = null;
                } elseif ($c === 0) {
                    $out[] = [];
                } else {
                    $out[] = \array_combine(\array_slice($keys, $off, $c), \array_slice($vals, $off, $c));
                    $off += $c;
                }
            }
        } else {
            foreach ($counts as $c) {
                if ($c < 0) {
                    $out[] = null;
                } elseif ($c === 0) {
                    $out[] = [];
                } else {
                    $out[] = \array_map(null, \array_slice($keys, $off, $c), \array_slice($vals, $off, $c));
                    $off += $c;
                }
            }
        }
        if ($off !== $total) {
            throw new FormatException("The lists below '{$s->leaves[0]->dottedPath}' hold $off elements, their column has $total");
        }
        return $out;
    }

    /**
     * A leaf's own column wrapped in the lists on its path (the projected shape: list levels kept, wrapper structs dropped).
     *
     * @param list<mixed> $col the entries at slot level $m, or the value to wrap
     * @param array<int, list<int>> $cnt
     * @return list<mixed> one entry per row
     */
    public static function wrap(array $col, array $cnt, int $fromLevel): array
    {
        for ($k = $fromLevel; $k >= 1; $k--) {
            $out = [];
            $off = 0;
            foreach ($cnt[$k] as $c) {
                if ($c < 0) {
                    $out[] = null;
                } elseif ($c === 0) {
                    $out[] = [];
                } else {
                    $out[] = \array_slice($col, $off, $c);
                    $off += $c;
                }
            }
            if ($off !== \count($col)) {
                throw new FormatException("The lists of a column hold $off elements, its column has " . \count($col));
            }
            $col = $out;
        }
        return $col;
    }
}
