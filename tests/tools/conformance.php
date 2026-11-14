<?php

declare(strict_types=1);

/**
 * Reads every file of the Apache parquet-testing corpus with the library and with pyarrow and compares every value.
 *
 *     php tests/tools/conformance.php [--mode=physical|logical] [--corpus=research/parquet-testing] [--python=research/venv/bin/python]
 *                                     [--filter=substring] [--timeout=60] [--verbose]
 *
 * One line per file: PASS | UNSUPPORTED <feature> | FAIL <first difference> | ERROR <unexpected Throwable or PHP warning>
 *                    | ORACLE-CANNOT-READ <pyarrow's error>
 * data/:     the file must read and equal pyarrow's values (UNSUPPORTED where the library says so).
 * bad_data/: the library must reject the file with FormatException (PASS) or UnsupportedException (UNSUPPORTED); reading it
 *            successfully is a FAIL, a hang is an ERROR. Each file is read in its own PHP process (dump.php) under a timeout.
 * Comparison (physical values, bit-exact floats, byte strings in hex) is explained in oracle.py. Exit status 1 on any FAIL or ERROR.
 */

$root = dirname(__DIR__, 2);
$opts = getopt('', ['corpus::', 'python::', 'filter::', 'timeout::', 'verbose', 'mode::']);
$mode = $opts['mode'] ?? 'physical';
if (!in_array($mode, ['physical', 'logical'], true)) {
    fwrite(STDERR, "--mode must be physical or logical\n");
    exit(2);
}
$corpus = $opts['corpus'] ?? "$root/research/parquet-testing";
$python = $opts['python'] ?? "$root/research/venv/bin/python";
$filter = $opts['filter'] ?? '';
$timeout = (int) ($opts['timeout'] ?? 60);
$verbose = isset($opts['verbose']);
$tmp = sys_get_temp_dir() . '/charm-parquet-conformance-' . getmypid();
mkdir($tmp);

if (!is_dir($corpus)) {
    fwrite(STDERR, "Corpus not found: $corpus (git clone --depth 1 https://github.com/apache/parquet-testing research/parquet-testing)\n");
    exit(2);
}
$havePython = is_file($python);
if (!$havePython) {
    fwrite(STDERR, "pyarrow oracle not found ($python): values are not compared.\n");
}

/** @return array{string, string} output, status */
function run(string $cmd, int $timeout): array
{
    $p = proc_open('timeout ' . $timeout . ' ' . $cmd, [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
    $out = stream_get_contents($pipes[1]) . stream_get_contents($pipes[2]);
    $code = proc_close($p);
    return [trim($out), (string) $code];
}

$counts = [];
$reasons = [];
$fail = false;
foreach (['data' => false, 'bad_data' => true] as $dir => $bad) {
    $files = [];
    foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator("$corpus/$dir", FilesystemIterator::SKIP_DOTS)) as $f) {
        if (str_ends_with($f->getFilename(), '.parquet') || str_ends_with($f->getFilename(), '.parquet.encrypted')) {
            $files[] = $f->getPathname();
        }
    }
    sort($files);
    foreach ($files as $file) {
        $name = "$dir/" . substr($file, strlen("$corpus/$dir/"));
        if ($filter !== '' && !str_contains($name, $filter)) {
            continue;
        }
        $dump = "$tmp/dump.json";
        @unlink($dump);
        [$o, $code] = run('php -d memory_limit=2G ' . escapeshellarg(__DIR__ . '/dump.php') . ' ' . escapeshellarg($file) . ' ' . escapeshellarg($dump) . ' ' . $mode, $timeout);
        $line = null;
        if ($code === '124') {
            $line = ['ERROR', "timeout after {$timeout}s"];
        } elseif (!is_file($dump)) {
            $line = ['ERROR', 'the reader process died (' . substr($o, 0, 200) . ')'];
        } else {
            $r = json_decode((string) file_get_contents($dump), true);
            $status = $r['status'];
            $unsupported = $r['unsupported'] ?? [];
            if ($r['message'] !== '' && $status === 'unsupported') {
                $unsupported = ['*' => $r['message']];
            }
            if ($status === 'error') {
                $line = ['ERROR', $r['message']];
            } elseif ($status === 'parquet') {
                $line = ['ERROR', 'ParquetException that is neither Format nor Unsupported: ' . $r['message']];
            } elseif ($status === 'format') {
                if ($bad) {
                    $line = ['PASS', 'rejected: ' . $r['message']];
                } else {
                    // the library calls the file corrupt: only acceptable if pyarrow cannot read it either
                    file_put_contents($dump, json_encode(['columns' => [], 'rows' => null]));
                    $v = $havePython ? trim(run(escapeshellarg($python) . ' ' . escapeshellarg(__DIR__ . '/oracle.py') . ' ' . escapeshellarg($file) . ' ' . escapeshellarg($dump), $timeout)[0]) : 'PASS';
                    $line = str_starts_with($v, 'ORACLE-CANNOT-READ') ? ['ORACLE-CANNOT-READ', 'both reject it: ' . $r['message']] : ['FAIL', 'FormatException on a file pyarrow reads: ' . $r['message']];
                }
            } else {
                // read (completely or in part): compare with pyarrow
                $v = 'PASS';
                if ($havePython) {
                    $v = trim(run(escapeshellarg($python) . ' ' . escapeshellarg(__DIR__ . '/oracle.py') . ' ' . escapeshellarg($file) . ' ' . escapeshellarg($dump), $timeout)[0]);
                }
                if ($bad && $unsupported === []) {
                    // read without any error: acceptable only if pyarrow reads the same values (then the file is odd, not unsafe)
                    $line = str_starts_with($v, 'PASS')
                        ? ['PASS', 'accepted; pyarrow reads the same values']
                        : ['FAIL', 'read without error from a bad_data file (pyarrow: ' . $v . ')'];
                } elseif ($unsupported !== [] && (str_starts_with($v, 'ORACLE-CANNOT-READ') || $r['columns'] === []) && !str_starts_with($v, 'FAIL')) {
                    $u = array_values(array_unique($unsupported));
                    sort($u);
                    $line = ['UNSUPPORTED', implode('; ', $u)];
                } elseif (str_starts_with($v, 'FAIL')) {
                    $line = ['FAIL', substr($v, 5)];
                } elseif (str_starts_with($v, 'ORACLE-CANNOT-READ')) {
                    $line = ['ORACLE-CANNOT-READ', substr($v, 19)];
                } elseif ($unsupported !== []) {
                    $u = array_values(array_unique($unsupported));
                    sort($u);
                    $line = ['UNSUPPORTED', implode('; ', $u)];
                } elseif (str_starts_with($v, 'PASS')) {
                    $line = ['PASS', trim(substr($v, 4))];
                } else {
                    $line = ['ERROR', 'the oracle script itself failed: ' . substr(str_replace("\n", ' | ', $v), -300)];
                }
            }
        }
        $counts[$dir][$line[0]] = ($counts[$dir][$line[0]] ?? 0) + 1;
        if ($line[0] === 'UNSUPPORTED') {
            foreach (array_unique(explode('; ', $line[1])) as $u) {
                // column names are not part of the reason
                $u = preg_replace("/^(nested column|encrypted column) .*/", '$1', $u);
                $u = preg_replace("/^column .* stored in another file.*/", 'column stored in another file', $u);
                $reasons[$dir][$u][$file] = true;
            }
        }
        if ($line[0] === 'FAIL' || $line[0] === 'ERROR') {
            $fail = true;
        }
        printf("%-18s %s%s\n", $line[0], $name, $line[1] !== '' ? ' -- ' . substr($line[1], 0, $verbose ? 1000 : 220) : '');
    }
}
@unlink("$tmp/dump.json");
@rmdir($tmp);

echo "\nSummary\n";
foreach ($counts as $dir => $c) {
    ksort($c);
    echo "  $dir: ", implode(', ', array_map(static fn ($k, $v) => "$k $v", array_keys($c), $c)), "\n";
}
echo "\nUNSUPPORTED reasons (files)\n";
foreach ($reasons as $dir => $rs) {
    uasort($rs, static fn ($a, $b) => count($b) <=> count($a));
    foreach ($rs as $reason => $files) {
        printf("  %-4d %-9s %s\n", count($files), $dir, $reason);
    }
}
exit($fail ? 1 : 0);
