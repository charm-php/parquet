<?php

declare(strict_types=1);

/**
 * Writes random files with charm/parquet and has this library's Reader, pyarrow and DuckDB read them back.
 *
 *     php tests/tools/roundtrip.php [--seeds=60] [--start=1] [--python=research/venv/bin/python] [--keep=dir]
 *
 * Every case is a seeded random schema (all types, nesting to depth 4, required and optional at every level) with random rows including edge values,
 * written with one of several configurations (codec, page version, dictionary, tiny pages and row groups, defaults). The expected values are
 * the input rows; each reader's result is compared with them (the Reader strictly, pyarrow and DuckDB through the canonical trees of oracle.py).
 * Prints one line per mismatch and a summary line; exit status 1 on any mismatch (a SKIP, a value the oracle cannot express, is not one).
 */

use Charm\Parquet\{Codec, Reader};
use Charm\Parquet\Tests\{Canonical, RandomData, WriterHelper};

require __DIR__ . '/../../vendor/autoload.php';

$root = dirname(__DIR__, 2);
$opts = getopt('', ['seeds::', 'start::', 'python::', 'keep::', 'extremes::']);
$seeds = (int) ($opts['seeds'] ?? 60);
$start = (int) ($opts['start'] ?? 1);
$python = $opts['python'] ?? "$root/research/venv/bin/python";
$dir = $opts['keep'] ?? sys_get_temp_dir() . '/charm-parquet-roundtrip-' . getmypid();
@mkdir($dir, 0777, true);

$configs = [
    'defaults' => [],
    'snappy v1 dict tiny' => ['codec' => Codec::Snappy, 'pageVersion' => 1, 'dictionary' => true, 'dataPageSize' => 64, 'rowGroupRows' => 7],
    'snappy v2 nodict tiny' => ['codec' => Codec::Snappy, 'pageVersion' => 2, 'dictionary' => false, 'dataPageSize' => 64, 'rowGroupRows' => 7],
    'gzip v1 tiny' => ['codec' => Codec::Gzip, 'pageVersion' => 1, 'dataPageSize' => 100, 'rowGroupRows' => 11],
    'zstd v2 dict tiny' => ['codec' => Codec::Zstd, 'pageVersion' => 2, 'dictionary' => true, 'dataPageSize' => 100, 'rowGroupRows' => 13],
    'uncompressed v1 one row per page' => ['codec' => Codec::Uncompressed, 'dataPageSize' => 1, 'rowGroupRows' => 3],
    'uncompressed v2 small dictionary' => ['codec' => Codec::Uncompressed, 'pageVersion' => 2, 'dataPageSize' => 80, 'dictionaryPageSize' => 48],
];

$gen = new RandomData();
$cases = [];
$problems = 0;
$n = 0;
for ($i = 0; $i < $seeds; $i++) {
    $seed = $start + $i;
    $schema = $gen->schema($seed);
    $rows = $gen->rows($schema, mt_rand(0, 80), $seed * 7919);
    $want = WriterHelper::normalizeRows($schema, $rows);
    foreach ($configs as $label => $options) {
        $file = sprintf('case-%d-%s.parquet', $seed, substr(md5($label), 0, 6));
        $bytes = WriterHelper::write($schema, $rows, $options);
        file_put_contents("$dir/$file", $bytes);
        $diff = WriterHelper::firstDifference($want, WriterHelper::read($bytes));
        if ($diff !== null) {
            echo "MISMATCH reader   seed $seed ($label): $diff\n";
            ++$problems;
        }
        $expected = [];
        foreach ($schema as $name => $_) {
            $expected[$name] = array_map(static fn (array $row) => Canonical::tree($row[$name]), $want);
        }
        $cases[] = ['file' => $file, 'label' => "seed $seed ($label)", 'expected' => $expected];
        ++$n;
    }
}
file_put_contents("$dir/cases.json", json_encode($cases, JSON_PRESERVE_ZERO_FRACTION));

$counts = ['pyarrow' => ['OK' => 0, 'SKIP' => 0, 'FAIL' => 0], 'duckdb' => ['OK' => 0, 'SKIP' => 0, 'FAIL' => 0, 'KNOWN' => 0]];
$labels = array_column($cases, 'label', 'file');
$p = popen(escapeshellarg($python) . ' ' . escapeshellarg(__DIR__ . '/roundtrip.py') . ' ' . escapeshellarg($dir) . ' 2>&1', 'r');
while (($line = fgets($p)) !== false) {
    $r = json_decode($line, true);
    if (!is_array($r)) {
        echo "PYTHON: $line";
        ++$problems;
        continue;
    }
    foreach (['pyarrow', 'duckdb', 'stats'] as $who) {
        $counts[$who] ??= ['OK' => 0, 'SKIP' => 0, 'FAIL' => 0, 'KNOWN' => 0];
        $status = explode(' ', $r[$who], 2)[0];
        $counts[$who][$status] = ($counts[$who][$status] ?? 0) + 1;
        if ($status !== 'OK') {
            echo sprintf("%-8s %-7s %s: %s\n", strtoupper($status), $who, $labels[$r['case']] ?? $r['case'], substr($r[$who], 0, 300));
            if ($status === 'FAIL') {
                ++$problems;
            }
        }
    }
}
pclose($p);
printf("statistics, indexes and range filters (pyarrow, DuckDB): %d OK, %d FAIL\n", $counts['stats']['OK'] ?? 0, $counts['stats']['FAIL'] ?? 0);
printf("roundtrip: %d files from %d seeds; own Reader: %d mismatches; pyarrow: %d OK, %d SKIP, %d FAIL; DuckDB: %d OK, %d SKIP, %d KNOWN (precision), %d FAIL\n",
    $n, $seeds, $problems - $counts['pyarrow']['FAIL'] - $counts['duckdb']['FAIL'] - ($counts['stats']['FAIL'] ?? 0), $counts['pyarrow']['OK'], $counts['pyarrow']['SKIP'], $counts['pyarrow']['FAIL'],
    $counts['duckdb']['OK'], $counts['duckdb']['SKIP'], $counts['duckdb']['KNOWN'], $counts['duckdb']['FAIL']);
if (!isset($opts['keep'])) {
    array_map('unlink', glob("$dir/*") ?: []);
    @rmdir($dir);
}
exit($problems > 0 ? 1 : 0);
