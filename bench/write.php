<?php

declare(strict_types=1);

/**
 * Benchmarks the Writer. Run it through run.php (php bench/run.php --write), which starts it with and without the JIT.
 *
 *     php bench/write.php [--runs=3] [--filter=text] [--codec=uncompressed,snappy,gzip,zstd] [--out=file.json]
 *
 * Each dataset is a file of bench/data (written by pyarrow, see make-data.py) read once into PHP rows with this library's Reader; the
 * workload is encode() for every row and finish() with the schema of that file (new Writer($reader->schema())) and default options except
 * the codec. "rows/s" counts input rows, "MB/s" megabytes (2^20) of OUTPUT per second. The time is the best of --runs after one warm-up.
 * bench/pyarrow-write.py writes the same data with pyarrow for the file sizes and, for context, the time.
 */

use Charm\Parquet\{Codec, Reader, Writer};

require __DIR__ . '/../vendor/autoload.php';

$opts = getopt('', ['runs::', 'filter::', 'codec::', 'out::']);
$runs = (int) ($opts['runs'] ?? 3);
$filter = (string) ($opts['filter'] ?? '');
$codecs = array_filter(explode(',', (string) ($opts['codec'] ?? 'uncompressed,snappy,gzip,zstd')));
$map = ['uncompressed' => Codec::Uncompressed, 'snappy' => Codec::Snappy, 'gzip' => Codec::Gzip, 'zstd' => Codec::Zstd];
$dir = __DIR__ . '/data';
$names = ['int64-plain', 'int64-dict', 'double', 'str-lowcard', 'str-highcard', 'opt-10pct-null', 'decimal', 'timestamp', 'list-int32-tokens', 'struct-3', 'wide-50'];

if (!is_file("$dir/int64-plain.snappy.parquet")) {
    fwrite(STDERR, "The benchmark data is missing; create it with research/venv/bin/python bench/make-data.py\n");
    exit(2);
}

$results = [];
printf("%-22s %-13s %9s %11s %9s %9s %12s\n", 'dataset', 'codec', 'rows', 'out bytes', 'ms', 'rows/s', 'MB/s (out)');
foreach ($names as $name) {
    if ($filter !== '' && !str_contains($name, $filter)) {
        continue;
    }
    $reader = Reader::fromFile("$dir/$name.snappy.parquet");
    $schema = $reader->schema();
    $rows = iterator_to_array($reader->rows(), false);
    foreach ($codecs as $cn) {
        $codec = $map[$cn];
        $once = static function () use ($schema, $rows, $codec): int {
            $w = new Writer($schema, $codec);
            $n = 0;
            foreach ($rows as $row) {
                $n += strlen($w->encode($row));
            }
            return $n + strlen($w->finish());
        };
        $bytes = $once();
        $best = INF;
        for ($i = 0; $i < $runs; $i++) {
            gc_collect_cycles();
            $t = hrtime(true);
            $bytes = $once();
            $best = min($best, (hrtime(true) - $t) / 1e6);
        }
        $n = count($rows);
        $results[$name][$cn] = ['ms' => $best, 'bytes' => $bytes, 'rows' => $n];
        printf("%-22s %-13s %9d %11d %9.0f %9.0f %12.1f\n", $name, $cn, $n, $bytes, $best, $n / ($best / 1000), $bytes / 1048576 / ($best / 1000));
    }
    unset($rows, $reader);
}
if (isset($opts['out'])) {
    file_put_contents($opts['out'], json_encode($results, JSON_PRETTY_PRINT));
}
