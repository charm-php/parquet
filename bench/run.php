<?php

declare(strict_types=1);

/**
 * One command: php bench/run.php [--write] [--runs=5] [--filter=text] [--codec=snappy|zstd] [--out=prefix]
 * With --write the Writer is benchmarked (write.php) instead of the Reader (bench.php); --codec then takes a comma list.
 * Runs bench.php twice, with the tracing JIT and without it (opcache on in both, so only the JIT differs).
 * With --out=prefix the results are also written to prefix.jit.json and prefix.nojit.json (see compare.php).
 */

$opts = getopt('', ['runs::', 'filter::', 'codec::', 'out::', 'write']);
$common = '-d opcache.enable_cli=1 -d opcache.file_update_protection=0 -d memory_limit=2G';
$php = escapeshellarg(PHP_BINARY);
$script = escapeshellarg(__DIR__ . (isset($opts['write']) ? '/write.php' : '/bench.php'));
$pass = '';
foreach (['runs', 'filter', 'codec'] as $k) {
    if (isset($opts[$k])) {
        $pass .= ' --' . $k . '=' . escapeshellarg((string) $opts[$k]);
    }
}
$out = $opts['out'] ?? null;

echo "# With the tracing JIT\n\n";
passthru("$php $common -d opcache.jit_buffer_size=128M -d opcache.jit=1255 $script$pass" . ($out ? ' --out=' . escapeshellarg("$out.jit.json") : ''), $a);
echo "\n# Without the JIT\n\n";
passthru("$php $common -d opcache.jit_buffer_size=0 $script$pass" . ($out ? ' --out=' . escapeshellarg("$out.nojit.json") : ''), $b);
exit($a ?: $b);
