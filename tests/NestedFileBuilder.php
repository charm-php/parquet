<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

/**
 * Builds nested Parquet files by hand from a schema description and the shredded data of each leaf: dense values, definition
 * levels and repetition levels, so that legacy list and map forms, rows that span pages and hostile level streams can be tested
 * without pyarrow (which does not write them).
 *
 * A schema node is ['name' =>, 'rep' => 0 required | 1 optional | 2 repeated, 'type' => physical type id (leaf) , 'children' => [...]
 * (group)], with optional 'conv' (converted type id) and 'len' (type length). Leaves are numbered depth first.
 */
final class NestedFileBuilder
{
    public const REQ = 0;
    public const OPT = 1;
    public const REP = 2;
    public const INT32 = 1;
    public const BYTE_ARRAY = 6;
    public const CONV_UTF8 = 0;
    public const CONV_MAP = 1;
    public const CONV_MAP_KEY_VALUE = 2;
    public const CONV_LIST = 3;

    /** @return array{name: string, rep: int, type: int, conv?: int} */
    public static function leaf(string $name, int $rep, int $type = self::INT32, ?int $conv = null): array
    {
        return ['name' => $name, 'rep' => $rep, 'type' => $type] + ($conv !== null ? ['conv' => $conv] : []);
    }

    /** @param list<array<string, mixed>> $children */
    public static function group(string $name, int $rep, array $children, ?int $conv = null): array
    {
        return ['name' => $name, 'rep' => $rep, 'children' => $children] + ($conv !== null ? ['conv' => $conv] : []);
    }

    /**
     * @param list<array<string, mixed>> $top the top-level fields
     * @param list<array{values: list<int|string>, defs?: list<int>, reps?: list<int>, split?: list<int>, version?: int, index?: list<int>}> $leaves shredded data, one per leaf in depth-first order;
     *        'split' lists the number of entries of each data page (default: one page), 'version' 2 writes data pages v2, 'index' gives the first row of each page for an offset index
     * @param array{numRows?: int, numValuesOverride?: list<int>, rowsOverride?: int} $opts
     */
    public static function file(array $top, array $leaves, int $rows, array $opts = []): string
    {
        $elements = [[[4, Thrift::BINARY, 'schema'], [5, Thrift::I32, \count($top)]]];
        $leafInfo = [];
        foreach ($top as $t) {
            self::walk($t, [], 0, 0, $elements, $leafInfo);
        }
        $body = 'PAR1';
        $chunks = [];
        foreach ($leafInfo as $i => $info) {
            $d = $leaves[$i] ?? throw new \InvalidArgumentException("no data for leaf $i");
            $reps = $d['reps'] ?? \array_fill(0, \count($d['defs'] ?? $d['values']), 0);
            $defs = $d['defs'] ?? \array_fill(0, \count($reps), $info['maxDef']);
            $n = \count($defs);
            $split = $d['split'] ?? [$n];
            $pages = '';
            $pageSizes = [];
            $vi = 0;
            $e = 0;
            foreach ($split as $cnt) {
                $pDefs = \array_slice($defs, $e, $cnt);
                $pReps = \array_slice($reps, $e, $cnt);
                $present = \count(\array_filter($pDefs, static fn (int $x): bool => $x === $info['maxDef']));
                $vals = \array_slice($d['values'], $vi, $present);
                $vi += $present;
                $e += $cnt;
                $page = self::page($info, $vals, $pDefs, $pReps, $d['version'] ?? 1);
                $pageSizes[] = \strlen($page);
                $pages .= $page;
            }
            $offset = \strlen($body);
            $body .= $pages;
            $indexFields = [];
            if (isset($d['index'])) {
                // an offset index with the first row of each page as given (the data may contradict it: hostile tests)
                $locs = [];
                $at = $offset;
                foreach ($d['index'] as $k => $firstRow) {
                    $locs[] = [[1, Thrift::I64, $at], [2, Thrift::I32, $pageSizes[$k]], [3, Thrift::I64, $firstRow]];
                    $at += $pageSizes[$k];
                }
                $idx = Thrift::struct([[1, Thrift::LIST, [Thrift::STRUCT, $locs]]]);
                $indexFields = [[4, Thrift::I64, \strlen($body)], [5, Thrift::I32, \strlen($idx)]];
                $body .= $idx;
            }
            $md = [
                [1, Thrift::I32, $info['type']],
                [2, Thrift::LIST, [Thrift::I32, [0]]],
                [3, Thrift::LIST, [Thrift::BINARY, $info['path']]],
                [4, Thrift::I32, 0],
                [5, Thrift::I64, $opts['numValuesOverride'][$i] ?? $n],
                [6, Thrift::I64, \strlen($pages)],
                [7, Thrift::I64, \strlen($pages)],
                [9, Thrift::I64, $offset],
            ];
            $chunks[] = [[2, Thrift::I64, $offset], [3, Thrift::STRUCT, $md], ...$indexFields];
        }
        $rg = [[1, Thrift::LIST, [Thrift::STRUCT, $chunks]], [2, Thrift::I64, \strlen($body)], [3, Thrift::I64, $opts['rowsOverride'] ?? $rows]];
        $footer = Thrift::struct([
            [1, Thrift::I32, 2],
            [2, Thrift::LIST, [Thrift::STRUCT, $elements]],
            [3, Thrift::I64, $rows],
            [4, Thrift::LIST, [Thrift::STRUCT, [$rg]]],
        ]);
        return $body . $footer . \pack('V', \strlen($footer)) . 'PAR1';
    }

    /** @param list<array<int, mixed>> $elements @param list<array<string, mixed>> $leafInfo */
    private static function walk(array $n, array $path, int $def, int $rep, array &$elements, array &$leafInfo): void
    {
        $def += $n['rep'] !== self::REQ ? 1 : 0;
        $rep += $n['rep'] === self::REP ? 1 : 0;
        $path[] = $n['name'];
        $el = [];
        if (isset($n['type'])) {
            $el[] = [1, Thrift::I32, $n['type']];
        }
        $el[] = [3, Thrift::I32, $n['rep']];
        $el[] = [4, Thrift::BINARY, $n['name']];
        if (isset($n['children'])) {
            $el[] = [5, Thrift::I32, \count($n['children'])];
        }
        if (isset($n['conv'])) {
            $el[] = [6, Thrift::I32, $n['conv']];
        }
        $elements[] = $el;
        if (isset($n['children'])) {
            foreach ($n['children'] as $c) {
                self::walk($c, $path, $def, $rep, $elements, $leafInfo);
            }
        } else {
            $leafInfo[] = ['type' => $n['type'], 'path' => $path, 'maxDef' => $def, 'maxRep' => $rep];
        }
    }

    private static function bitWidth(int $max): int
    {
        $w = 0;
        while ($max > 0) {
            ++$w;
            $max >>= 1;
        }
        return $w;
    }

    /** RLE runs of equal levels (no bit-packing). */
    public static function rle(array $levels, int $max): string
    {
        $bytes = \intdiv(self::bitWidth($max) + 7, 8);
        $out = '';
        $i = 0;
        $n = \count($levels);
        while ($i < $n) {
            $j = $i;
            while ($j < $n && $levels[$j] === $levels[$i]) {
                ++$j;
            }
            $out .= Thrift::varint(($j - $i) << 1) . \substr(\pack('V', $levels[$i]), 0, $bytes);
            $i = $j;
        }
        return $out;
    }

    private static function page(array $info, array $vals, array $defs, array $reps, int $version): string
    {
        $values = '';
        foreach ($vals as $v) {
            $values .= $info['type'] === self::BYTE_ARRAY ? \pack('V', \strlen($v)) . $v : \pack('V', $v & 0xFFFFFFFF);
        }
        $nv = \count($defs);
        $repL = $info['maxRep'] > 0 ? self::rle($reps, $info['maxRep']) : '';
        $defL = $info['maxDef'] > 0 ? self::rle($defs, $info['maxDef']) : '';
        if ($version === 2) {
            $body = $repL . $defL . $values;
            $h = Thrift::struct([
                [1, Thrift::I32, 3],
                [2, Thrift::I32, \strlen($body)],
                [3, Thrift::I32, \strlen($body)],
                [8, Thrift::STRUCT, [[1, Thrift::I32, $nv], [2, Thrift::I32, $nv - \count($vals)], [3, Thrift::I32, \count(\array_filter($reps, static fn (int $r): bool => $r === 0))],
                    [4, Thrift::I32, 0], [5, Thrift::I32, \strlen($defL)], [6, Thrift::I32, \strlen($repL)], [7, Thrift::BOOL_TRUE, false]]],
            ]);
            return $h . $body;
        }
        $body = ($info['maxRep'] > 0 ? \pack('V', \strlen($repL)) . $repL : '') . ($info['maxDef'] > 0 ? \pack('V', \strlen($defL)) . $defL : '') . $values;
        return FileBuilder::dataPageV1($body, $nv);
    }
}
