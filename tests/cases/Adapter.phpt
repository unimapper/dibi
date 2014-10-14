<?php

use Tester\Assert;

require __DIR__ . '/../bootstrap.php';

/**
 * @skip
 */
class AdapterTest extends Tester\TestCase
{

    /** @var \UniMapper\Dibi\Adapter $adapter */
    private $adapter;

    public function setUp()
    {
        $this->adapter = new UniMapper\Dibi\Adapter("test", new DibiConnection([]));
    }

    public function testCreateDelete()
    {
        $query = $this->adapter->createDelete("table");
        Assert::same("", $query->getRaw());
    }

}

$testCase = new AdapterTest;
$testCase->run();