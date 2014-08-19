<?php

use Tester\Assert;

require __DIR__ . '/../bootstrap.php';

class AdapterTest extends Tester\TestCase
{

    /** @var \Mockery\Mock $connectionMock */
    private $connectionMock;

    /** @var \UniMapper\Dibi\Adapter $adapter */
    private $adapter;

    public function setUp()
    {
        $this->connectionMock = Mockery::mock("DibiConnection");
        $this->adapter = new UniMapper\Dibi\Adapter("test", $this->connectionMock);
    }

    public function testConvertConditions()
    {
        Assert::same(
            array(
                'AND',
                '(%n %sql %i OR (%n %sql %s AND (%n %sql %s OR %n %sql %s) OR %n %sql %s))',
                array(
                    'id',
                    '=',
                    4,
                    'text',
                    'LIKE',
                    'yetAnotherFoo2',
                    'text',
                    'LIKE',
                    'yetAnotherFoo3',
                    'text',
                    'LIKE',
                    'yetAnotherFoo4',
                    'text',
                    'LIKE',
                    'yetAnotherFoo5',
                )
            ),
            $this->adapter->convertCondition(
                array(
                    array(
                        array('id', '=', 4, 'AND'),
                        array(
                            array(
                                array('text', 'LIKE', 'yetAnotherFoo2', 'AND'),
                                array(
                                    array(
                                        array('text', 'LIKE', 'yetAnotherFoo3', 'OR'),
                                        array('text', 'LIKE', 'yetAnotherFoo4', 'OR'),
                                    ),
                                    'AND'
                                ),
                                array('text', 'LIKE', 'yetAnotherFoo5', 'OR')
                            ),
                            'OR'
                        )
                    ),
                    'AND'
                )
            )
        );
    }

    public function testFind()
    {
        $this->connectionMock->shouldReceive("select")->with("[col1],[col2]")->andReturnSelf();
        $this->connectionMock->shouldReceive("from")->with("%n", "table")->andReturnSelf();
        $this->connectionMock->shouldReceive("limit")->with("%i", 10)->andReturnSelf();
        $this->connectionMock->shouldReceive("offset")->with("%i", 20)->andReturnSelf();
        $this->connectionMock->shouldReceive("where")->with("%n %sql %i", "col1", "=", 1)->andReturnSelf();
        $this->connectionMock->shouldReceive("orderBy")->with("col1")->andReturnSelf();
        $this->connectionMock->shouldReceive("desc")->andReturnSelf();
        $this->connectionMock->shouldReceive("fetchAll")->andReturn([]);

        $this->adapter->find(
            "table",
            ["col1", "col2"],
            [["col1", "=", 1, "AND"]],
            ["col1" => "desc"],
            10,
            20,
            []
        );
    }

}

$testCase = new AdapterTest;
$testCase->run();