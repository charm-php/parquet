<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Codec, Encoding, PageType};


/**
 * Builds small Parquet files by hand, with chosen (and, for the hostile-file tests, wrong) sizes and counts.
 * One row group; the columns are given as ['name' => ..., 'type' => physical type id, 'optional' => bool, 'pages' => string (the chunk bytes)].
 */
final class FileBuilder
{
    /**
     * A page: header and body. $header overrides fields of the PageHeader (by id) after the defaults.
     *
     * @param array<int, array{int, mixed}> $override id => [type, value]
     */
    public static function dataPageV1(string $body, int $numValues, int $encoding = 0, ?int $uncompressedSize = null, ?int $compressedSize = null, array $override = []): string
    {
        $fields = [
            1 => [Thrift::I32, PageType::DataPage->value],
            2 => [Thrift::I32, $uncompressedSize ?? \strlen($body)],
            3 => [Thrift::I32, $compressedSize ?? \strlen($body)],
            5 => [Thrift::STRUCT, [[1, Thrift::I32, $numValues], [2, Thrift::I32, $encoding], [3, Thrift::I32, 3], [4, Thrift::I32, 3]]],
        ];
        return self::page($fields, $override, $body);
    }

    /**
     * A data page v2: the levels (uncompressed) in front of the values.
     *
     * @param array<int, array{int, mixed}> $override
     */
    public static function dataPageV2(string $defLevels, string $values, int $numValues, int $numNulls, int $encoding = 0, array $override = []): string
    {
        $body = $defLevels . $values;
        $fields = [
            1 => [Thrift::I32, PageType::DataPageV2->value],
            2 => [Thrift::I32, \strlen($body)],
            3 => [Thrift::I32, \strlen($body)],
            8 => [Thrift::STRUCT, [[1, Thrift::I32, $numValues], [2, Thrift::I32, $numNulls], [3, Thrift::I32, $numValues], [4, Thrift::I32, $encoding],
                [5, Thrift::I32, \strlen($defLevels)], [6, Thrift::I32, 0], [7, Thrift::BOOL_TRUE, false]]],
        ];
        return self::page($fields, $override, $body);
    }

    /** @param array<int, array{int, mixed}> $override */
    public static function dictionaryPage(string $body, int $numValues, array $override = []): string
    {
        $fields = [
            1 => [Thrift::I32, PageType::DictionaryPage->value],
            2 => [Thrift::I32, \strlen($body)],
            3 => [Thrift::I32, \strlen($body)],
            7 => [Thrift::STRUCT, [[1, Thrift::I32, $numValues], [2, Thrift::I32, Encoding::Plain->value]]],
        ];
        return self::page($fields, $override, $body);
    }

    /**
     * @param array<int, array{int, mixed}> $fields
     * @param array<int, array{int, mixed}> $override
     */
    private static function page(array $fields, array $override, string $body): string
    {
        foreach ($override as $id => $f) {
            $fields[$id] = $f;
        }
        \ksort($fields);
        $list = [];
        foreach ($fields as $id => [$type, $value]) {
            $list[] = [$id, $type, $value];
        }
        return Thrift::struct($list) . $body;
    }

    /** RLE/bit-packed hybrid of definition levels, 1 bit wide, with the 4-byte length prefix of data page v1. */
    public static function defLevelsV1(array $bits): string
    {
        $runs = '';
        foreach ($bits as $b) {
            $runs .= \chr($b);
        }
        // one bit-packed run covering all values (padded to a multiple of 8)
        $groups = \intdiv(\count($bits) + 7, 8);
        $packed = '';
        for ($g = 0; $g < $groups; $g++) {
            $byte = 0;
            for ($i = 0; $i < 8; $i++) {
                $byte |= (($bits[$g * 8 + $i] ?? 0) & 1) << $i;
            }
            $packed .= \chr($byte);
        }
        $enc = Thrift::varint(($groups << 1) | 1) . $packed;
        return \pack('V', \strlen($enc)) . $enc;
    }

    /**
     * @param list<array{name: string, type: int, optional?: bool, chunk: string, numValues?: int, codec?: int, totalCompressed?: int, offset?: int, noMetadata?: bool, crypto?: bool, statistics?: list<array{int, int, mixed}>, offsetIndex?: string, offsetIndexAt?: int, offsetIndexLength?: int, columnIndex?: string, columnIndexAt?: int, columnIndexLength?: int}> $columns
     * @param array{numRows?: int, footerLength?: int, magic?: string, schema?: list<list<array{int, int, mixed}>>, footerExtra?: list<array{int, int, mixed}>} $opts
     */
    public static function file(array $columns, int $numRows, array $opts = []): string
    {
        $body = 'PAR1';
        $schema = [[[4, Thrift::BINARY, 'schema'], [5, Thrift::I32, \count($columns)]]];
        $chunks = [];
        foreach ($columns as $c) {
            $offset = $c['offset'] ?? \strlen($body);
            $body .= $c['chunk'];
            $idxFields = [];
            if (isset($c['offsetIndex'])) {
                $idxFields[] = [4, Thrift::I64, $c['offsetIndexAt'] ?? \strlen($body)];
                $idxFields[] = [5, Thrift::I32, $c['offsetIndexLength'] ?? \strlen($c['offsetIndex'])];
                $body .= $c['offsetIndex'];
            }
            if (isset($c['columnIndex'])) {
                $idxFields[] = [6, Thrift::I64, $c['columnIndexAt'] ?? \strlen($body)];
                $idxFields[] = [7, Thrift::I32, $c['columnIndexLength'] ?? \strlen($c['columnIndex'])];
                $body .= $c['columnIndex'];
            }
            $el = [[1, Thrift::I32, $c['type']], [3, Thrift::I32, ($c['optional'] ?? false) ? 1 : 0], [4, Thrift::BINARY, $c['name']]];
            if ($c['type'] === 7) {
                \array_splice($el, 1, 0, [[2, Thrift::I32, $c['typeLength'] ?? 4]]);
            }
            $schema[] = $el;
            $md = [
                [1, Thrift::I32, $c['type']],
                [2, Thrift::LIST, [Thrift::I32, [0]]],
                [3, Thrift::LIST, [Thrift::BINARY, [$c['name']]]],
                [4, Thrift::I32, $c['codec'] ?? Codec::Uncompressed->value],
                [5, Thrift::I64, $c['numValues'] ?? $numRows],
                [6, Thrift::I64, \strlen($c['chunk'])],
                [7, Thrift::I64, $c['totalCompressed'] ?? \strlen($c['chunk'])],
                [9, Thrift::I64, $offset],
                ...(isset($c['statistics']) ? [[12, Thrift::STRUCT, $c['statistics']]] : []),
            ];
            if ($c['noMetadata'] ?? false) {
                $chunks[] = \array_merge([[2, Thrift::I64, $offset]], ($c['crypto'] ?? false) ? [[8, Thrift::STRUCT, []], [9, Thrift::BINARY, 'secret']] : []);
            } else {
                $chunks[] = [[2, Thrift::I64, $offset], [3, Thrift::STRUCT, $md], ...$idxFields, ...(($c['crypto'] ?? false) ? [[8, Thrift::STRUCT, []]] : [])];
            }
        }
        $footer = Thrift::struct([
            [1, Thrift::I32, 2],
            [2, Thrift::LIST, [Thrift::STRUCT, $opts['schema'] ?? $schema]],
            [3, Thrift::I64, $opts['numRows'] ?? $numRows],
            [4, Thrift::LIST, [Thrift::STRUCT, [[[1, Thrift::LIST, [Thrift::STRUCT, $chunks]], [2, Thrift::I64, \strlen($body)], [3, Thrift::I64, $numRows]]]]],
            ...($opts['footerExtra'] ?? []),
        ]);
        return $body . $footer . \pack('V', $opts['footerLength'] ?? \strlen($footer)) . ($opts['magic'] ?? 'PAR1');
    }
}
