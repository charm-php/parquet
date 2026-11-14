<?php

declare(strict_types=1);

/**
 * Compresses test inputs with the Writer's Snappy compressor and has the reference python `snappy` package and pyarrow's Snappy codec decode them.
 *
 *     php tests/tools/snappy.php [--python=research/venv/bin/python]
 *
 * Prints a summary line; exit status 1 on any mismatch.
 */

use Charm\Parquet\Internal\Write\SnappyEncoder;
use Charm\Parquet\Tests\WriterFormatTest;

require __DIR__ . '/../../vendor/autoload.php';

$root = dirname(__DIR__, 2);
$opts = getopt('', ['python::']);
$python = $opts['python'] ?? "$root/research/venv/bin/python";
$inputs = WriterFormatTest::snappyInputs();
mt_srand(1234);
for ($i = 0; $i < 300; $i++) {
    $n = mt_rand(0, 200000);
    $alphabet = mt_rand(1, 256);
    $s = '';
    while (strlen($s) < $n) {
        $s .= mt_rand(0, 3) === 0 ? str_repeat(chr(mt_rand(0, $alphabet - 1)), mt_rand(1, 300)) : (strlen($s) > 100 && mt_rand(0, 1) ? substr($s, mt_rand(0, strlen($s) - 50), mt_rand(4, 200)) : random_bytes_seeded(mt_rand(1, 50), $alphabet));
    }
    $inputs["random $i"] = substr($s, 0, $n);
}
function random_bytes_seeded(int $n, int $alphabet): string
{
    $s = '';
    for ($i = 0; $i < $n; $i++) {
        $s .= chr(mt_rand(0, $alphabet - 1));
    }
    return $s;
}
$dir = sys_get_temp_dir() . '/charm-parquet-snappy-' . getmypid();
mkdir($dir);
$k = 0;
$total = [0, 0];
foreach ($inputs as $name => $in) {
    $c = SnappyEncoder::compress($in);
    file_put_contents("$dir/$k.raw", $in);
    file_put_contents("$dir/$k.snappy", $c);
    $total[0] += strlen($in);
    $total[1] += strlen($c);
    ++$k;
}
$py = <<<'PY'
import sys, glob, os
import snappy, pyarrow as pa
d = sys.argv[1]
n = len(glob.glob(d + "/*.raw"))
bad_ref = bad_pa = 0
for i in range(n):
    raw = open("%s/%d.raw" % (d, i), "rb").read()
    c = open("%s/%d.snappy" % (d, i), "rb").read()
    try:
        if snappy.decompress(c) != raw:
            bad_ref += 1
    except Exception:
        bad_ref += 1
    try:
        if pa.decompress(c, decompressed_size=len(raw), codec="snappy").to_pybytes() != raw:
            bad_pa += 1
    except Exception:
        bad_pa += 1
print(n, bad_ref, bad_pa)
PY;
file_put_contents("$dir/check.py", $py);
$out = trim((string) shell_exec(escapeshellarg($python) . ' ' . escapeshellarg("$dir/check.py") . ' ' . escapeshellarg($dir) . ' 2>&1'));
array_map('unlink', glob("$dir/*") ?: []);
rmdir($dir);
if (!preg_match('/^(\d+) (\d+) (\d+)$/', $out, $m)) {
    echo "python failed: $out\n";
    exit(1);
}
printf("snappy: %d inputs (%d bytes in, %d out); python-snappy mismatches: %d; pyarrow mismatches: %d\n", $m[1], $total[0], $total[1], $m[2], $m[3]);
exit($m[2] + $m[3] > 0 ? 1 : 0);
