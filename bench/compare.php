<?php

declare(strict_types=1);

/**
 * php bench/compare.php before.jit.json after.jit.json: the time of every workload in two runs (see run.php --out=) and the change.
 * A positive percentage is faster. Differences under about 5% are noise on a shared machine.
 */

if ($argc < 3) {
    fwrite(STDERR, "usage: php bench/compare.php before.json after.json\n");
    exit(2);
}
$a = json_decode((string) file_get_contents($argv[1]), true);
$b = json_decode((string) file_get_contents($argv[2]), true);
printf("%-38s %10s %10s %8s\n", 'workload', 'before ms', 'after ms', 'faster');
foreach ($a as $k => $x) {
    if (!isset($b[$k])) {
        continue;
    }
    printf("%-38s %10.1f %10.1f %7.1f%%\n", str_replace('|', ' ', $k), $x['ms'], $b[$k]['ms'], ($x['ms'] / $b[$k]['ms'] - 1) * 100);
}
