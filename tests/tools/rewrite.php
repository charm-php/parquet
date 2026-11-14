<?php

declare(strict_types=1);

/**
 * Rewrites every readable file of the Apache parquet-testing corpus (data/) with charm/parquet's Writer and checks the result.
 *
 *     php tests/tools/rewrite.php [--corpus=research/parquet-testing] [--python=research/venv/bin/python] [--filter=text] [--keep=dir]
 *
 * For each file: the Reader reads it (a file the Reader cannot read is "not readable" and not counted), `new Writer($reader->schema())`
 * writes every row back (default options), this library's Reader reads the result and compares it with the original rows (strictly:
 * PHP types, NaN, the sign of zero), then pyarrow and DuckDB read the rewritten file and are compared with the original rows
 * (tests/tools/roundtrip.py: values, statistics, page indexes, range filters).
 * Outcomes: REWRITTEN OK | UNSUPPORTED <reason from the Writer> | MISMATCH <first difference> | ERROR <anything else>.
 * Prints a line per file and a table; exit status 1 on any MISMATCH or ERROR (UNSUPPORTED is a documented limit, not a failure).
 */

use Charm\Parquet\{FormatException, Reader, UnsupportedException, ValueMode, Writer};
use Charm\Parquet\Tests\{Canonical, WriterHelper};

require __DIR__ . '/../../vendor/autoload.php';

$root = dirname(__DIR__, 2);
$opts = getopt('', ['corpus::', 'python::', 'filter::', 'keep::', 'one:', 'out:']);

if (isset($opts['one'])) {
    // worker: one file, result as JSON on stdout
    set_error_handler(static function (int $no, string $msg, string $file, int $line): never {
        throw new ErrorException($msg, 0, $no, $file, $line);
    });
    $result = ['status' => 'error', 'message' => ''];
    try {
        $in = Reader::fromFile($opts['one'], ValueMode::Logical);
        try {
            $rows = iterator_to_array($in->rows(), false);
        } catch (UnsupportedException|FormatException $e) {
            echo json_encode(['status' => 'unreadable', 'message' => get_class($e) . ': ' . $e->getMessage()]);
            exit(0);
        }
        try {
            $w = new Writer($in->schema());
        } catch (UnsupportedException $e) {
            echo json_encode(['status' => 'unsupported', 'message' => $e->getMessage()]);
            exit(0);
        }
        $fh = fopen($opts['out'], 'wb');
        try {
            foreach ($rows as $row) {
                fwrite($fh, $w->encode($row));
            }
            fwrite($fh, $w->finish());
        } catch (UnsupportedException $e) {
            echo json_encode(['status' => 'unsupported', 'message' => $e->getMessage()]);
            exit(0);
        } finally {
            fclose($fh);
        }
        $back = iterator_to_array(Reader::fromFile($opts['out'], ValueMode::Logical)->rows(), false);
        $diff = WriterHelper::firstDifference($rows, $back);
        $expected = [];
        foreach ($in->schema()->root->children as $top) {
            $expected[$top->name] = array_map(static fn (array $r) => Canonical::tree($r[$top->name] ?? null), $rows);
        }
        $result = ['status' => $diff === null ? 'ok' : 'mismatch', 'message' => (string) $diff, 'rows' => count($rows), 'expected' => $expected,
            'in' => filesize($opts['one']), 'out' => filesize($opts['out'])];
    } catch (Throwable $e) {
        $result = ['status' => 'error', 'message' => get_class($e) . ': ' . $e->getMessage() . ' at ' . basename($e->getFile()) . ':' . $e->getLine()];
    }
    echo json_encode($result, JSON_PRESERVE_ZERO_FRACTION | JSON_INVALID_UTF8_SUBSTITUTE);
    exit(0);
}

$corpus = $opts['corpus'] ?? "$root/research/parquet-testing";
$python = $opts['python'] ?? "$root/research/venv/bin/python";
$filter = $opts['filter'] ?? '';
$dir = $opts['keep'] ?? sys_get_temp_dir() . '/charm-parquet-rewrite-' . getmypid();
@mkdir($dir, 0777, true);
if (!is_dir("$corpus/data")) {
    fwrite(STDERR, "Corpus not found: $corpus\n");
    exit(2);
}
$files = [];
foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator("$corpus/data", FilesystemIterator::SKIP_DOTS)) as $f) {
    if (str_ends_with($f->getFilename(), '.parquet')) {
        $files[] = $f->getPathname();
    }
}
sort($files);

$table = ['REWRITTEN OK' => 0, 'UNSUPPORTED' => 0, 'MISMATCH' => 0, 'ERROR' => 0, 'not readable' => 0];
$reasons = [];
$cases = [];
$labels = [];
$problems = 0;
$sizes = [];
foreach ($files as $i => $file) {
    $name = substr($file, strlen("$corpus/data/"));
    if ($filter !== '' && !str_contains($name, $filter)) {
        continue;
    }
    $out = "$dir/" . sprintf('%03d', $i) . '.parquet';
    $p = proc_open('timeout 120 php -d memory_limit=2G ' . escapeshellarg(__FILE__) . ' --one=' . escapeshellarg($file) . ' --out=' . escapeshellarg($out),
        [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
    $o = stream_get_contents($pipes[1]);
    $e = stream_get_contents($pipes[2]);
    proc_close($p);
    $r = json_decode($o, true);
    if (!is_array($r)) {
        $r = ['status' => 'error', 'message' => 'the worker died: ' . substr(trim($e . $o), 0, 200)];
    }
    switch ($r['status']) {
        case 'unreadable':
            $table['not readable']++;
            echo "NOT-READABLE $name: {$r['message']}\n";
            break;
        case 'unsupported':
            $table['UNSUPPORTED']++;
            $reasons[$r['message']][] = $name;
            echo "UNSUPPORTED  $name: {$r['message']}\n";
            break;
        case 'mismatch':
            $table['MISMATCH']++;
            ++$problems;
            echo "MISMATCH     $name: {$r['message']}\n";
            break;
        case 'ok':
            $name2 = basename($out);
            $cases[] = ['file' => $name2, 'label' => $name, 'expected' => $r['expected']];
            $labels[$name2] = $name;
            $sizes[$name] = [$r['in'], $r['out']];
            break;
        default:
            $table['ERROR']++;
            ++$problems;
            echo "ERROR        $name: {$r['message']}\n";
    }
}
file_put_contents("$dir/cases.json", json_encode($cases, JSON_PRESERVE_ZERO_FRACTION));

$ext = ['pyarrow' => ['OK' => 0, 'SKIP' => 0, 'FAIL' => 0, 'KNOWN' => 0], 'duckdb' => ['OK' => 0, 'SKIP' => 0, 'FAIL' => 0, 'KNOWN' => 0], 'stats' => ['OK' => 0, 'SKIP' => 0, 'FAIL' => 0, 'KNOWN' => 0]];
$failed = [];
if ($cases !== [] && is_file($python)) {
    $p = popen(escapeshellarg($python) . ' ' . escapeshellarg(__DIR__ . '/roundtrip.py') . ' ' . escapeshellarg($dir) . ' 2>&1', 'r');
    while (($line = fgets($p)) !== false) {
        $r = json_decode($line, true);
        if (!is_array($r)) {
            echo "PYTHON: $line";
            ++$problems;
            continue;
        }
        $bad = false;
        foreach (['pyarrow', 'duckdb', 'stats'] as $who) {
            $status = explode(' ', $r[$who], 2)[0];
            $ext[$who][$status] = ($ext[$who][$status] ?? 0) + 1;
            if ($status !== 'OK') {
                echo sprintf("%-8s %-8s %s: %s\n", strtoupper($status), $who, $labels[$r['case']] ?? $r['case'], substr($r[$who], 0, 300));
            }
            if ($status === 'FAIL') {
                $bad = true;
                ++$problems;
            }
        }
        $failed[$r['case']] = $bad;
    }
    pclose($p);
} else {
    fwrite(STDERR, "pyarrow/DuckDB not available: only the own-Reader comparison was done\n");
}
$table['REWRITTEN OK'] = count($cases) - count(array_filter($failed));
$table['MISMATCH'] += count(array_filter($failed));

echo "\nOutcome                         files\n";
foreach ($table as $k => $v) {
    printf("%-30s %5d\n", $k, $v);
}
if ($reasons !== []) {
    echo "\nUnsupported, by reason:\n";
    foreach ($reasons as $reason => $names) {
        printf("  %3d  %s  (%s)\n", count($names), $reason, implode(', ', array_slice($names, 0, 3)));
    }
}
printf("\nrewrite: %d files rewritten and compared with the original rows; pyarrow: %d OK, %d SKIP, %d FAIL; DuckDB: %d OK, %d SKIP, %d KNOWN, %d FAIL; statistics/indexes/filters: %d OK, %d FAIL\n",
    count($cases), $ext['pyarrow']['OK'], $ext['pyarrow']['SKIP'], $ext['pyarrow']['FAIL'], $ext['duckdb']['OK'], $ext['duckdb']['SKIP'], $ext['duckdb']['KNOWN'], $ext['duckdb']['FAIL'], $ext['stats']['OK'], $ext['stats']['FAIL']);
if (!isset($opts['keep'])) {
    array_map('unlink', glob("$dir/*") ?: []);
    @rmdir($dir);
}
exit($problems > 0 ? 1 : 0);
