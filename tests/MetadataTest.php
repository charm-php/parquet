<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Codec, ConvertedType, Encoding, LogicalKind, PhysicalType, Repetition, SchemaNode, TimeUnit};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** The footer model against what pyarrow reports for the same file (expected/*.json "metadata"). */
final class MetadataTest extends TestCase
{
    /** @return array<string, array{string}> */
    public static function fixtures(): array
    {
        $out = [];
        foreach (Fixtures::names() as $n) {
            $out[$n] = [$n];
        }
        return $out;
    }

    private static function snake(string $case): string
    {
        return \strtoupper(\preg_replace('/(?<!^)(?=[A-Z]|\d)/', '_', $case));
    }

    private static function physicalName(PhysicalType $t): string
    {
        return match ($t) {
            PhysicalType::FixedLenByteArray => 'FIXED_LEN_BYTE_ARRAY',
            PhysicalType::ByteArray => 'BYTE_ARRAY',
            default => \strtoupper($t->name),
        };
    }

    private static function codecName(Codec $c): string
    {
        // (pyarrow reports the LZ4_RAW codec, id 7, as 'LZ4')
        return $c === Codec::Lz4Raw ? 'LZ4' : \strtoupper($c->name);
    }

    private static function encodingName(Encoding $e): string
    {
        return match ($e) {
            Encoding::Plain => 'PLAIN',
            Encoding::PlainDictionary => 'PLAIN_DICTIONARY',
            Encoding::Rle => 'RLE',
            Encoding::BitPacked => 'BIT_PACKED',
            Encoding::DeltaBinaryPacked => 'DELTA_BINARY_PACKED',
            Encoding::DeltaLengthByteArray => 'DELTA_LENGTH_BYTE_ARRAY',
            Encoding::DeltaByteArray => 'DELTA_BYTE_ARRAY',
            Encoding::RleDictionary => 'RLE_DICTIONARY',
            Encoding::ByteStreamSplit => 'BYTE_STREAM_SPLIT',
            Encoding::Alp => 'ALP',
        };
    }

    #[DataProvider('fixtures')]
    public function testFooterEqualsPyarrowsView(string $name): void
    {
        $want = Fixtures::expected($name)['metadata'];
        $m = Fixtures::reader($name)->metadata();

        $this->assertSame($want['numRows'], $m->numRows);
        $this->assertSame($want['numRowGroups'], \count($m->rowGroups));
        $this->assertSame($want['createdBy'], $m->createdBy);
        $this->assertSame($want['version'] === '1.0' ? 1 : 2, $m->version);
        $keys = \array_map('strval', \array_keys($m->keyValueMetadata));
        \sort($keys);
        $this->assertSame($want['keyValueKeys'], $keys);

        $this->assertCount(\count($want['leaves']), $m->schema->leaves);
        foreach ($m->schema->leaves as $i => $leaf) {
            $w = $want['leaves'][$i];
            $this->assertSame($w['path'], $leaf->dottedPath);
            $this->assertSame(\implode('.', $leaf->path), $leaf->dottedPath);
            $this->assertSame($w['physical'], self::physicalName($leaf->physicalType), $leaf->dottedPath);
            $this->assertSame($w['maxDef'], $leaf->maxDefinitionLevel, $leaf->dottedPath);
            $this->assertSame($w['maxRep'], $leaf->maxRepetitionLevel, $leaf->dottedPath);
            $this->assertSame($i, $leaf->leafIndex);
            $this->assertSame($w['logical'] !== null, $leaf->logicalType !== null, $leaf->dottedPath . ': has a logical type');
        }

        foreach ($m->rowGroups as $g => $rg) {
            $wg = $want['rowGroups'][$g];
            $this->assertSame($wg['numRows'], $rg->numRows);
            $this->assertCount(\count($wg['columns']), $rg->columns);
            foreach ($rg->columns as $c => $chunk) {
                $wc = $wg['columns'][$c];
                $this->assertSame($wc['path'], $chunk->leaf->dottedPath);
                $this->assertSame($wc['codec'], self::codecName($chunk->codec), "$name {$wc['path']}");
                $encs = \array_map(self::encodingName(...), $chunk->encodings);
                \sort($encs);
                $this->assertSame($wc['encodings'], $encs, "$name {$wc['path']} encodings");
                $this->assertSame($wc['numValues'], $chunk->numValues);
                $this->assertSame($wc['hasDictionaryPage'], $chunk->dictionaryPageOffset !== null && $chunk->dictionaryPageOffset > 0, "$name {$wc['path']}: dictionary page");
                if ($wc['nullCount'] !== null) {
                    $this->assertSame($wc['nullCount'], $chunk->statistics?->nullCount);
                }
                $this->assertGreaterThanOrEqual(0, $chunk->totalCompressedSize);
            }
        }
    }

    public function testNestedSchemaIsATree(): void
    {
        $s = Fixtures::reader('nested')->schema();
        $root = $s->root;
        $this->assertNull($root->repetition);
        $this->assertSame('', $root->dottedPath);
        $this->assertSame(['id', 'st', 'reqst', 'reqgroup', 'lst', 'mp', 'deep'], \array_map(static fn (SchemaNode $c) => $c->name, $root->children));

        $st = $root->children[1];
        $this->assertFalse($st->isLeaf());
        $this->assertSame(Repetition::Optional, $st->repetition);
        $this->assertSame(['x', 'y'], \array_map(static fn (SchemaNode $c) => $c->name, $st->children));
        $this->assertSame(['st', 'y'], $st->children[1]->path);
        $this->assertSame(2, $st->children[1]->maxDefinitionLevel);

        $lst = $root->children[4];
        $this->assertSame(ConvertedType::List, $lst->convertedType);
        $this->assertSame(LogicalKind::List, $lst->logicalType?->kind);
        $list = $lst->children[0];
        $this->assertSame(Repetition::Repeated, $list->repetition);
        $el = $list->children[0];
        $this->assertSame('lst.list.element', $el->dottedPath);
        $this->assertSame(1, $el->maxRepetitionLevel);
        $this->assertSame(3, $el->maxDefinitionLevel);
        $this->assertFalse($el->isFlat());

        $mp = $root->children[5];
        $this->assertSame(LogicalKind::Map, $mp->logicalType?->kind);
        $kv = $mp->children[0];
        $this->assertSame(Repetition::Repeated, $kv->repetition);
        $this->assertSame(['key', 'value'], \array_map(static fn (SchemaNode $c) => $c->name, $kv->children));
        $this->assertSame(Repetition::Required, $kv->children[0]->repetition);
        $this->assertSame(PhysicalType::ByteArray, $kv->children[0]->physicalType);
        $this->assertSame(LogicalKind::String, $kv->children[0]->logicalType?->kind);

        $this->assertSame($el, $s->leaf('lst.list.element'));
        $this->assertNull($s->leaf('lst'));
        $this->assertSame(7, \count($root->children));
        $this->assertTrue($s->leaf('reqgroup.x')->isFlat());
        $this->assertFalse($s->leaf('st.x')->isFlat());
    }

    public function testLogicalTypesAndTheirParameters(): void
    {
        $s = Fixtures::reader('logical')->schema();
        $u16 = $s->leaf('u16');
        $this->assertSame(LogicalKind::Integer, $u16->logicalType->kind);
        $this->assertSame(16, $u16->logicalType->bitWidth);
        $this->assertFalse($u16->logicalType->isSigned);
        $this->assertSame(ConvertedType::Uint16, $u16->convertedType);
        $this->assertSame(PhysicalType::Int32, $u16->physicalType);
        $this->assertSame(64, $s->leaf('u64')->logicalType->bitWidth);
        $this->assertTrue($s->leaf('i8')->logicalType->isSigned);
        $this->assertSame(LogicalKind::Date, $s->leaf('date')->logicalType->kind);
        $t = $s->leaf('t64')->logicalType;
        $this->assertSame(LogicalKind::Time, $t->kind);
        $this->assertSame(TimeUnit::Micros, $t->unit);
        $this->assertSame(TimeUnit::Millis, $s->leaf('t32')->logicalType->unit);
        $ts = $s->leaf('ts')->logicalType;
        $this->assertSame(LogicalKind::Timestamp, $ts->kind);
        $this->assertTrue($ts->isAdjustedToUtc);
        $this->assertSame(TimeUnit::Micros, $ts->unit);
        $d = $s->leaf('dec_flba');
        $this->assertSame(LogicalKind::Decimal, $d->logicalType->kind);
        $this->assertSame(10, $d->logicalType->precision);
        $this->assertSame(2, $d->logicalType->scale);
        $this->assertSame(10, $d->precision);
        $this->assertSame(2, $d->scale);
        $this->assertSame(PhysicalType::FixedLenByteArray, $d->physicalType);
        $this->assertSame(5, $d->typeLength);
        $this->assertSame(LogicalKind::Float16, $s->leaf('f16')->logicalType->kind);
        $this->assertSame(2, $s->leaf('f16')->typeLength);

        $di = Fixtures::reader('decimal-int')->schema();
        $this->assertSame(PhysicalType::Int32, $di->leaf('d9')->physicalType);
        $this->assertSame(PhysicalType::Int64, $di->leaf('d18')->physicalType);
        $this->assertSame(4, $di->leaf('d18')->logicalType->scale);
        $this->assertSame(PhysicalType::Int96, Fixtures::reader('int96')->schema()->leaf('ts')->physicalType);
    }

    public function testKeyValueMetadataAndStatistics(): void
    {
        $m = Fixtures::reader('kv-metadata')->metadata();
        $this->assertSame('tests', $m->keyValueMetadata['owner']);
        $this->assertSame("k\u{e9}y", $m->keyValueMetadata['note']);
        $this->assertStringStartsWith('parquet-cpp-arrow', (string) $m->createdBy);
        $chunk = null;
        foreach ($m->rowGroups[0]->columns as $c) {
            if ($c->leaf->dottedPath === 'r_int32') {
                $chunk = $c;
            }
        }
        $this->assertNotNull($chunk?->statistics);
        $this->assertSame(0, $chunk->statistics->nullCount);
        $this->assertSame(4, \strlen((string) $chunk->statistics->minValue));
        $this->assertSame(4, \strlen((string) $chunk->statistics->maxValue));
        $this->assertLessThanOrEqual($chunk->statistics->max === null ? 0 : \unpack('l', $chunk->statistics->max)[1], \unpack('l', (string) $chunk->statistics->maxValue)[1]);
    }

    public function testDictionaryChunksRecordTheirDictionaryPage(): void
    {
        $c = Fixtures::reader('matrix-snappy-v1-dict')->metadata()->rowGroups[0]->columns[2]; // r_int32 (booleans are never dictionary encoded)
        $this->assertNotNull($c->dictionaryPageOffset);
        $this->assertSame($c->dictionaryPageOffset, $c->startOffset());
        $p = Fixtures::reader('matrix-snappy-v1-plain')->metadata()->rowGroups[0]->columns[0];
        $this->assertSame($p->dataPageOffset, $p->startOffset());
    }
}
