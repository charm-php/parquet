<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\{Reader, TimeUnit, Type, Writer};
use PHPUnit\Framework\TestCase;

/**
 * The PHP examples of the README that are marked `<!-- example: name -->` are run, in a temporary directory, against fixtures made
 * here; the signature listing is checked against the classes by reflection. An example that shows an API which does not exist fails.
 */
final class ReadmeExamplesTest extends TestCase
{
    private string $dir;
    private string $cwd;

    protected function setUp(): void
    {
        $this->cwd = (string) getcwd();
        $this->dir = sys_get_temp_dir() . '/charm-parquet-readme-' . getmypid() . '-' . bin2hex(random_bytes(3));
        mkdir($this->dir);
        chdir($this->dir);
    }

    protected function tearDown(): void
    {
        chdir($this->cwd);
        foreach (glob($this->dir . '/*') ?: [] as $f) {
            unlink($f);
        }
        rmdir($this->dir);
    }

    /** @return array<string, string> example name => code */
    private static function examples(): array
    {
        $md = (string) file_get_contents(__DIR__ . '/../README.md');
        preg_match_all('/<!-- example: (\w+) -->\n```php\n(.*?)\n```/s', $md, $m, PREG_SET_ORDER);
        $out = [];
        foreach ($m as [, $name, $code]) {
            $out[$name] = $code;
        }
        return $out;
    }

    /** @param array<string, mixed> $vars */
    private static function execute(string $code, array $vars): void
    {
        (static function () use ($code, $vars): void {
            extract($vars);
            eval($code);
        })();
    }

    private function eventsFile(): void
    {
        $w = new Writer([
            'id' => Type::int64()->required(), 'user' => Type::struct(['name' => Type::string(), 'tags' => Type::list(Type::string())]),
            'at' => Type::timestamp(TimeUnit::Micros, utc: true),
        ], rowGroupRows: 600, dataPageSize: 200);
        $out = '';
        for ($i = 0; $i < 3000; $i++) {
            $out .= $w->encode(['id' => $i, 'user' => ['name' => "u$i", 'tags' => $i % 3 === 0 ? [] : ['a', 'b']], 'at' => '2024-05-01T12:00:00.000000Z']);
        }
        file_put_contents('events.parquet', $out . $w->finish());
    }

    public function testAllMarkedExamplesExist(): void
    {
        $this->assertSame(['reader', 'http', 'writer', 'infer'], array_keys(self::examples()));
    }

    public function testReaderExample(): void
    {
        $this->eventsFile();
        ob_start();
        self::execute(self::examples()['reader'], []);
        $out = ob_get_clean();
        $this->assertStringContainsString('3000 rows in 5 row groups', $out);
        $this->assertStringContainsString('user.tags.list.element ByteArray def 4 rep 1', $out);
    }

    public function testHttpExampleDefinesItsFunction(): void
    {
        self::execute(self::examples()['http'], []);
        $this->assertTrue(function_exists('parquetOverHttp'));
        $this->assertSame(Reader::class, (string) (new \ReflectionFunction('parquetOverHttp'))->getReturnType());
    }

    public function testWriterExample(): void
    {
        $rows = [
            ['id' => 1, 'name' => 'Ann', 'price' => '9.50', 'seen' => '2024-05-01T12:00:00.000000Z', 'tags' => ['a'], 'address' => ['city' => 'Oslo', 'zip' => '0150']],
            ['id' => 2, 'name' => null, 'price' => null, 'seen' => null, 'tags' => [], 'address' => null],
        ];
        self::execute(self::examples()['writer'], ['rows' => $rows]);
        $back = iterator_to_array(Reader::fromFile('out.parquet')->rows(), false);
        $this->assertSame($rows, $back);
    }

    public function testInferExample(): void
    {
        $rows = [['id' => 1, 'name' => 'Ann', 'tags' => ['a']], ['id' => 2, 'name' => null, 'tags' => []]];
        self::execute(self::examples()['infer'], ['rows' => $rows]);
        $this->assertSame($rows, iterator_to_array(Reader::fromFile('out.parquet')->rows(), false));
    }

    public function testSignatureListingNamesRealMethods(): void
    {
        $md = (string) file_get_contents(__DIR__ . '/../README.md');
        $this->assertSame(1, preg_match('/```php\nnamespace Charm\\\\Parquet;\n\nfinal class Reader\n\{(.*?)\n\}\n/s', $md, $m));
        preg_match_all('/public (?:static )?function (\w+)\(/', $m[1], $names);
        $this->assertGreaterThan(12, count($names[1]));
        foreach ($names[1] as $name) {
            $this->assertTrue(method_exists(Reader::class, $name), "Reader::$name is in the README but not in the class");
        }
        // and the other way: every public method of the Reader is listed
        foreach ((new \ReflectionClass(Reader::class))->getMethods(\ReflectionMethod::IS_PUBLIC) as $method) {
            $this->assertContains($method->getName(), $names[1], "Reader::{$method->getName()}() is not in the README listing");
        }
    }
}
