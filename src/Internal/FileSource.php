<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\ParquetException;

/** The range function over a local file. The handle is opened on the first read and closed when this object is destroyed. */
final class FileSource
{
    /** @var resource|null */
    private $h = null;

    public function __construct(private readonly string $path)
    {
    }

    public function size(): int
    {
        $s = @\filesize($this->path);
        if ($s === false) {
            throw new ParquetException("Cannot open {$this->path}");
        }
        return $s;
    }

    public function read(int $offset, int $length): string
    {
        if ($this->h === null) {
            $h = @\fopen($this->path, 'rb');
            if ($h === false) {
                throw new ParquetException("Cannot open {$this->path}");
            }
            $this->h = $h;
        }
        if ($length === 0) {
            return '';
        }
        if (\fseek($this->h, $offset) !== 0) {
            throw new ParquetException("Cannot seek to $offset in {$this->path}");
        }
        $out = '';
        while (\strlen($out) < $length) {
            $part = \fread($this->h, $length - \strlen($out));
            if ($part === false || $part === '') {
                break; // (the caller reports a short read)
            }
            $out .= $part;
        }
        return $out;
    }

    public function __destruct()
    {
        if ($this->h !== null) {
            \fclose($this->h);
        }
    }
}
