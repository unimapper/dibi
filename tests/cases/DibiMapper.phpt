<?php

use Tester\Assert;


require __DIR__ . '/../bootstrap.php';

$connectionMock = $mockista->create("DibiConnection");

$mapper = new UniMapper\Mapper\DibiMapper("dibimapper", $connectionMock);


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
    $mapper->convertCondition(
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