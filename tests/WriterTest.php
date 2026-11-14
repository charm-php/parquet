<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Codec, Type};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Writer output read back by this package's Reader: random schemas and rows, every codec, page version and layout. */
final class WriterTest extends TestCase
{
    /** @return array<string, array{int}> */
    public static function seeds(): array
    {
        $out = [];
        for ($i = 1; $i <= 40; $i++) {
            $out["seed $i"] = [$i];
        }
        return $out;
    }

    /** @return list<array<string, mixed>> */
    public static function configurations(): array
    {
        $c = [['codec' => Codec::Snappy], []];
        foreach ([Codec::Uncompressed, Codec::Snappy, Codec::Gzip, Codec::Zstd] as $codec) {
            foreach ([1, 2] as $v) {
                foreach ([true, false] as $dict) {
                    $c[] = ['codec' => $codec, 'pageVersion' => $v, 'dictionary' => $dict, 'dataPageSize' => 64, 'rowGroupRows' => 7];
                }
            }
        }
        $c[] = ['dataPageSize' => 1, 'rowGroupRows' => 1, 'dictionarySize' => 0];
        return $c;
    }

    #[DataProvider('seeds')]
    public function testRandomSchemasAndRowsReadBackIdentical(int $seed): void
    {
        $gen = new RandomData();
        $schema = $gen->schema($seed);
        $rows = $gen->rows($schema, \mt_rand(0, 60), $seed * 7919);
        $want = WriterHelper::normalizeRows($schema, $rows);
        foreach (self::configurations() as $k => $options) {
            unset($options['dictionarySize']);
            $options['dictionaryPageSize'] = ($options['dataPageSize'] ?? 0) === 1 ? 40 : 1 << 20;
            $bytes = WriterHelper::write($schema, $rows, $options);
            $diff = WriterHelper::firstDifference($want, WriterHelper::read($bytes));
            $this->assertNull($diff, "seed $seed configuration $k " . \json_encode(\array_map(static fn ($v) => $v instanceof \UnitEnum ? $v->name : $v, $options)) . ': ' . $diff);
        }
    }
}
