<?php

use Tester\Assert;
use UniMapper\Entity\Filter;

require __DIR__ . '/../bootstrap.php';

/**
 * @testCase
 *
 * @dataProvider common/db.ini
 */
class QueryTest extends Tester\TestCase
{

    /** @var UniMapper\Dibi\Adapter $query */
    private $query;

    public function __construct($config)
    {
        $this->query = new UniMapper\Dibi\Query(new DibiFluent(new DibiConnection($config)));
    }

    public function testConvertFilterString()
    {
        Assert::same(
            array(array('%n %sql %s', 'id', '=', 'foo')),
            $this->query->convertFilter(
                ["id" => [Filter::EQUAL => "foo"]]
            )
        );
    }

    public function testConvertFilterInteger()
    {
        Assert::same(
            array(
                array('%n %sql %i', 'id', '=', 1),
                array('%n %sql %i', 'id', '>', 2),
                array('%n %sql %i', 'id', '<', 3),
                array('%n %sql %i', 'id', '>=', 4),
                array('%n %sql %i', 'id', '<=', 5),
                array('%n %sql %i', 'id', '!=', 6),
            ),
            $this->query->convertFilter(
                [
                    "id" => [
                        Filter::EQUAL => 1,
                        Filter::GREATER => 2,
                        Filter::LESS => 3,
                        Filter::GREATEREQUAL => 4,
                        Filter::LESSEQUAL => 5,
                        Filter::NOT => 6
                    ]
                ]
            )
        );
    }

    public function testConvertFilterIn()
    {
        Assert::same(
            array(array('%n %sql %in', 'id', 'IN', array(1, 2))),
            $this->query->convertFilter(
                ["id" => [Filter::EQUAL => [1, 2]]]
            )
        );
    }

    public function testConvertFilterNotIn()
    {
        Assert::same(
            array(array('%n %sql %in', 'id', 'NOT IN', array(1, 2))),
            $this->query->convertFilter(
                ["id" => [Filter::NOT => [1, 2]]]
            )
        );
    }

    public function testConvertFilterBoolean()
    {
        Assert::same(
            array(array('%n %sql %b', 'true', 'IS', true)),
            $this->query->convertFilter(["true" => [Filter::EQUAL => true]])
        );
        Assert::same(
            array(array('%n %sql %b', 'true', 'IS NOT', true)),
            $this->query->convertFilter(["true" => [Filter::NOT => true]])
        );
        Assert::same(
            array(array('%n %sql %b', 'false', 'IS', false)),
            $this->query->convertFilter(["false" => [Filter::EQUAL => false]])
        );
        Assert::same(
            array(array('%n %sql %b', 'false', 'IS NOT', false)),
            $this->query->convertFilter(["false" => [Filter::NOT => false]])
        );
    }

    public function testConvertFilterNull()
    {
        Assert::same(
            array(array('%n %sql %sN', 'null', 'IS', null)),
            $this->query->convertFilter(["null" => [Filter::EQUAL => null]])
        );
        Assert::same(
            array(array('%n %sql %sN', 'null', 'IS NOT', null)),
            $this->query->convertFilter(["null" => [Filter::NOT => null]])
        );
    }

    public function testConvertFilterDate()
    {
        $date = new UniMapper\Dibi\Date(new DateTime("1999-12-31"));
        Assert::same(
            array(array('%n %sql %d', 'date', '=', $date)),
            $this->query->convertFilter(["date" => [Filter::EQUAL => $date]])
        );
    }

    public function testConvertFilterTime()
    {
        $time = new DateTime("1999-12-31");
        Assert::same(
            array(array('%n %sql %t', 'time', '=', $time)),
            $this->query->convertFilter(["time" => [Filter::EQUAL => $time]])
        );
    }

    public function testConvertFilterDouble()
    {
        Assert::same(
            array(array('%n %sql %f', 'double', '=', 1.1)),
            $this->query->convertFilter(["double" => [Filter::EQUAL => 1.1]])
        );
    }

    public function testConvertFilterLike()
    {
        Assert::same(
            array(
                array('%n %sql %like~', 'start', 'LIKE', 'start'),
                array('%n %sql %~like', 'end', 'LIKE', 'end'),
                array('%n %sql %~like~', 'contain', 'LIKE', 'contain'),
            ),
            $this->query->convertFilter(
                [
                    "start" => [Filter::START => "start"],
                    "end" => [Filter::END => "end"],
                    "contain" => [Filter::CONTAIN => "contain"]
                ]
            )
        );
    }

    public function testConvertFilterGroup()
    {
        Assert::same(
            array(
                array(
                    '%and',
                    array(
                        array('%n %sql %i', 'one', '=', 1),
                        array('%n %sql %i', 'two', '=', 2),
                    ),
                ),
                array(
                    '%and',
                    array(
                        array('%n %sql %i', 'three', '=', 3),
                        array('%n %sql %i', 'four', '=', 4),
                    ),
                ),
            ),
            $this->query->convertFilter(
                [
                    [
                        "one" => [Filter::EQUAL => 1],
                        "two" => [Filter::EQUAL => 2]
                    ],
                    [
                        "three" => [Filter::EQUAL => 3],
                        "four" => [Filter::EQUAL => 4]
                    ]
                ]
            )
        );
    }

    public function testConvertFilterGroupWithOr()
    {
        $filter = [
            [
                Filter::_OR => [
                    "one" => [Filter::EQUAL => 1],
                    "two" => [Filter::EQUAL => 2]
                ]
            ],
            [
                "three" => [Filter::EQUAL => 3],
                "four" => [Filter::EQUAL => 4]
            ]
        ];
        Assert::same(
            array(
                array(
                    '%and',
                    array(
                        array(
                            '%or',
                            array(
                                array('%n %sql %i', 'one', '=', 1),
                                array('%n %sql %i', 'two', '=', 2),
                            ),
                        ),
                    ),
                ),
                array(
                    '%and',
                    array(
                        array('%n %sql %i', 'three', '=', 3),
                        array('%n %sql %i', 'four', '=', 4),
                    ),
                ),
            ),
            $this->query->convertFilter($filter)
        );

        $this->query->setFilter($filter);
        Assert::same(
            "WHERE ((([one] = 1) OR ([two] = 2))) AND (([three] = 3) AND ([four] = 4))",
            $this->query->getRaw()
        );
    }

    public function testConvertFilterGroupNested()
    {
        $filter = [
            Filter::_OR => [
                [
                    "one" => [Filter::EQUAL => 1],
                    "two" => [Filter::EQUAL => 2]
                ],
                [
                    "three" => [Filter::EQUAL => 3],
                    "four" => [Filter::EQUAL => 4]
                ]
            ]
        ];

        Assert::same(
            array(
                array(
                    '%or',
                    array(
                        array(
                            '%and',
                            array(
                                array('%n %sql %i', 'one', '=', 1),
                                array('%n %sql %i', 'two', '=', 2),
                            ),
                        ),
                        array(
                            '%and',
                            array(
                                array('%n %sql %i', 'three', '=', 3),
                                array('%n %sql %i', 'four', '=', 4),
                            ),
                        ),
                    ),
                ),
            ),
            $this->query->convertFilter($filter)
        );

        $this->query->setFilter($filter);
        Assert::same(
            "WHERE ((([one] = 1) AND ([two] = 2)) OR (([three] = 3) AND ([four] = 4)))",
            $this->query->getRaw()
        );
    }

    public function testSetFilter()
    {
        $this->query->setFilter(["foo" => [Filter::EQUAL => 1]]);
        Assert::same("WHERE ([foo] = 1)", $this->query->getRaw());
    }

}

$testCase = new QueryTest($config);
$testCase->run();