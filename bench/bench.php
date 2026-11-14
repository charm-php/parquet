<?php

declare(strict_types=1);

/**
 * Benchmarks charm/parquet on files written by pyarrow. Run it through run.php, which starts this script with and without the JIT:
 *
 *     research/venv/bin/python bench/make-data.py    # once: writes bench/data/*.parquet (needs pyarrow)
 *     php bench/run.php
 *
 * Options: --runs=5  --filter=text (workload name contains)  --codec=snappy|zstd  --out=file.json
 * Each workload reads every column of every row group once (readColumn for one column, rows() for the wide table), after one
 * warm-up read; the time is the best of --runs. "MB/s" is megabytes (2^20) of FILE per second, "Mval/s" million values (level entries
 * for nested columns) per second. Values are in logical mode (the default) unless the workload name says physical.
 */

use Charm\Parquet\{Reader, ValueMode};

require __DIR__ . '/../vendor/autoload.php';

$opts = getopt('', ['runs::', 'filter::', 'codec::', 'out::']);
$runs = (int) ($opts['runs'] ?? 5);
$filter = (string) ($opts['filter'] ?? '');
$codecs = isset($opts['codec']) ? [(string) $opts['codec']] : ['snappy', 'zstd'];
$dir = __DIR__ . '/data';

if (!is_file("$dir/int64-plain.snappy.parquet")) {
    fwrite(STDERR, "The benchmark data is missing. Create it once with pyarrow:\n\n"
        . "    python3 -m venv research/venv && research/venv/bin/pip install pyarrow\n"
        . "    research/venv/bin/python bench/make-data.py\n\n"
        . "It writes about 120 MB into bench/data/ (git-ignored).\n");
    exit(2);
}

/** @return array{float, int} best time in ms and the number of values read */
function measure(\Closure $fn, int $runs): array
{
    $n = $fn(); // warm-up: lets the tracing JIT see the hot loops
    $best = INF;
    for ($i = 0; $i < $runs; $i++) {
        gc_collect_cycles();
        $t = hrtime(true);
        $n = $fn();
        $best = min($best, (hrtime(true) - $t) / 1e6);
    }
    return [$best, $n];
}

// name => [file stem, ValueMode, closure(Reader): int number of values read]
$wholeColumn = static function (Reader $r): int {
    $n = 0;
    for ($g = 0, $c = $r->rowGroupCount(); $g < $c; $g++) {
        $r->readColumn($g, 'v');
        foreach ($r->metadata()->rowGroups[$g]->columns as $chunk) {
            $n += $chunk->numValues; // (values: level entries, so a list of 10 ints counts 10)
        }
    }
    return $n;
};
$workloads = [];
foreach (['int64-plain', 'int64-dict', 'int64-delta', 'double', 'str-lowcard', 'str-highcard', 'str-delta', 'opt-10pct-null', 'opt-90pct-null',
    'decimal', 'timestamp', 'list-int32-tokens', 'list-float-128', 'struct-3'] as $w) {
    $workloads[$w] = [$w, ValueMode::Logical, $wholeColumn];
}
$workloads['decimal (physical)'] = ['decimal', ValueMode::Physical, $wholeColumn];
$workloads['timestamp (physical)'] = ['timestamp', ValueMode::Physical, $wholeColumn];
$allCols = static function (Reader $r): int {
    $n = 0;
    foreach ($r->rows() as $row) {
        $n += \count($row);
    }
    return $n;
};
$threeCols = static function (Reader $r): int {
    $n = 0;
    foreach ($r->rows(['c00', 'c02', 'c03']) as $row) {
        $n += \count($row);
    }
    return $n;
};
$workloads['wide-50, rows() all columns'] = ['wide-50', ValueMode::Logical, $allCols];
$workloads['wide-50, rows() 3 columns'] = ['wide-50', ValueMode::Logical, $threeCols];

$jit = ini_get('opcache.jit_buffer_size');
$jitOn = $jit !== '0' && $jit !== '' && $jit !== false;
printf("PHP %s, JIT %s, %s, best of %d\n\n", PHP_VERSION, $jitOn ? 'on (' . ini_get('opcache.jit') . ')' : 'off', php_uname('s') . ' ' . php_uname('r'), $runs);
printf("%-30s %-7s %9s %9s %9s %9s\n", 'workload', 'codec', 'ms', 'MB/s', 'Mval/s', 'file MB');
printf("%'-30s-%'-7s-%'-9s-%'-9s-%'-9s-%'-9s\n", '', '', '', '', '', '');
$results = [];
foreach ($workloads as $name => [$stem, $mode, $fn]) {
    if ($filter !== '' && !str_contains($name, $filter)) {
        continue;
    }
    foreach ($codecs as $codec) {
        $file = "$dir/$stem.$codec.parquet";
        $size = filesize($file);
        $reader = Reader::fromFile($file, $mode);
        [$ms, $n] = measure(static fn () => $fn($reader), $runs);
        $mb = $size / 1048576;
        printf("%-30s %-7s %9.1f %9.1f %9.2f %9.2f\n", $name, $codec, $ms, $mb / ($ms / 1000), $n / 1e6 / ($ms / 1000), $mb);
        $results["$name|$codec"] = ['ms' => $ms, 'values' => $n, 'bytes' => $size];
    }
}
if (isset($opts['out'])) {
    file_put_contents((string) $opts['out'], json_encode($results, JSON_PRETTY_PRINT));
}
