<?php

declare(strict_types=1);

namespace Charm\Parquet;

final readonly class Schema
{
    /** @var list<SchemaNode> the leaves in schema order */
    public array $leaves;
    /** @var array<string, SchemaNode> leaves by dotted path */
    private array $byPath;
    /** @var array<string, SchemaNode> every node below the root by dotted path */
    private array $nodesByPath;

    public function __construct(public SchemaNode $root)
    {
        $leaves = [];
        // depth-first, children in order (the order the column chunks are stored in)
        $collect = static function (SchemaNode $n) use (&$collect, &$leaves): void {
            if ($n->isLeaf()) {
                $leaves[] = $n;
                return;
            }
            foreach ($n->children as $c) {
                $collect($c);
            }
        };
        $collect($root);
        $by = [];
        foreach ($leaves as $l) {
            $by[$l->dottedPath] ??= $l;
        }
        $all = [];
        $walk = static function (SchemaNode $n) use (&$walk, &$all): void {
            foreach ($n->children as $c) {
                $all[$c->dottedPath] ??= $c;
                $walk($c);
            }
        };
        $walk($root);
        $this->leaves = $leaves;
        $this->byPath = $by;
        $this->nodesByPath = $all;
    }

    /** The node (group or leaf) with this dotted path, or null. */
    public function node(string $dottedPath): ?SchemaNode
    {
        return $this->nodesByPath[$dottedPath] ?? null;
    }

    /** The leaf with this dotted path, or null. */
    public function leaf(string $dottedPath): ?SchemaNode
    {
        return $this->byPath[$dottedPath] ?? null;
    }
}
