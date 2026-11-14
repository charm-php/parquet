<?php

declare(strict_types=1);

/**
 * Reads one Parquet file with the library and writes what it read as JSON, for tests/tools/oracle.py to compare with pyarrow.
 *
 *     php tests/tools/dump.php FILE OUT.json
 *
 * Values are written in a canonical text form that survives JSON (so floats, byte strings and 64-bit integers are exact):
 *   BOOLEAN "t"/"f"; INT32/INT64 decimal digits; FLOAT 8 hex digits and DOUBLE 16 hex digits (the big-endian IEEE bit pattern);
 *   BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 the bytes in hex; null as JSON null.
 * With a third argument `logical` the reader is in ValueMode::Logical and values are written by PHP type instead (Canonical::logical).
 * Each leaf column is read on its own, row group by row group, so that one unsupported column does not hide the others.
 * Exit status 0 whenever a result was written (the "status" says what happened); PHP warnings and non-Parquet Throwables are
 * recorded as status "error".
 */

use Charm\Parquet\{ParquetException, Reader, UnsupportedException, ValueMode};
use Charm\Parquet\Tests\Canonical;

require __DIR__ . '/../../vendor/autoload.php';

set_error_handler(static function (int $no, string $msg, string $file, int $line): never {
    throw new ErrorException($msg, 0, $no, $file, $line);
});

$logical = ($argv[3] ?? '') === 'logical';
$result = ['mode' => $logical ? 'logical' : 'physical', 'status' => 'ok', 'message' => '', 'rows' => null, 'columns' => [], 'unsupported' => []];
$out = $argv[2] ?? null;
try {
    $reader = Reader::fromFile($argv[1], $logical ? ValueMode::Logical : ValueMode::Physical);
    $result['rows'] = array_sum(array_map(static fn ($g) => $g->numRows, $reader->metadata()->rowGroups)); // (the footer's own num_rows is wrong in some corpus files)
    // flat columns one leaf at a time (so that one unsupported column does not hide the others); every other top-level field as a
    // whole, assembled, in the canonical tree form
    $result['trees'] = [];
    foreach ($reader->schema()->root->children as $top) {
        try {
            if (!$top->isLeaf() || !$top->isFlat()) {
                $rows = [];
                for ($g = 0, $n = $reader->rowGroupCount(); $g < $n; $g++) {
                    foreach ($reader->readColumn($g, $top->name) as $v) {
                        $rows[] = Canonical::tree($v);
                    }
                }
                $result['trees'][$top->name] = $rows;
            }
        } catch (UnsupportedException $e) {
            $result['unsupported'][$top->name] = $e->getMessage();
        }
    }
    foreach ($reader->schema()->leaves as $leaf) {
        $path = $leaf->dottedPath;
        if (!$leaf->isFlat()) {
            continue;
        }
        $vals = [];
        try {
            for ($g = 0, $n = $reader->rowGroupCount(); $g < $n; $g++) {
                $col = $reader->readColumn($g, $path);
                foreach ($col as $v) {
                    $vals[] = $logical ? Canonical::logical($v) : Canonical::value($leaf->physicalType, $v);
                }
            }
            $result['columns'][$path] = ['physical' => $leaf->physicalType?->name, 'typeLength' => $leaf->typeLength, 'values' => $vals];
        } catch (UnsupportedException $e) {
            $result['unsupported'][$path] = $e->getMessage();
        }
    }
    if ($result['unsupported'] !== []) {
        $result['status'] = 'unsupported';
    }
} catch (UnsupportedException $e) {
    $result['status'] = 'unsupported';
    $result['message'] = $e->getMessage();
} catch (ParquetException $e) {
    $result['status'] = 'format';
    $result['message'] = $e->getMessage();
} catch (Throwable $e) {
    $result['status'] = 'error';
    $result['message'] = get_class($e) . ': ' . $e->getMessage() . ' at ' . basename($e->getFile()) . ':' . $e->getLine();
}
file_put_contents($out ?? 'php://stdout', json_encode($result, JSON_INVALID_UTF8_SUBSTITUTE));
