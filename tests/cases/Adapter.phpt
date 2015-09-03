<?php

use Tester\Assert;

require __DIR__ . '/../bootstrap.php';

/**
 * @dataProvider common/db.ini
 */
class AdapterTest extends Tester\TestCase
{

    /** @var \UniMapper\Dibi\Adapter $adapter */
    private $adapter;

    public function __construct(array $config)
    {
        $this->adapter = new UniMapper\Dibi\Adapter($config);
    }

    public function testCreateDelete()
    {
        Assert::same(
            "DELETE FROM [table]",
            $this->adapter->createDelete("table")->getRaw()
        );
    }

    public function testCreateDeleteOne()
    {
        Assert::same(
            "DELETE FROM [table] WHERE [id] = '1'",
            $this->adapter->createDeleteOne("table", "id", 1)->getRaw()
        );
    }

}

$testCase = new AdapterTest($config);
$testCase->run();