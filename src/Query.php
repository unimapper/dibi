<?php

namespace UniMapper\Dibi;

use UniMapper\Entity\Filter;

class Query implements \UniMapper\Adapter\IQuery
{

    public $associations = [];

    /** @var \DibiFluent */
    public $fluent;

    public $resultCallback;

    /** @var array $modificators Dibi modificators */
    private $modificators = [
        "boolean" => "%b",
        "integer" => "%i",
        "string" => "%s",
        "NULL" => "%sN",
        "DateTime" => "%t",
        "UniMapper\Dibi\Date" => "%d",
        "array" => "%in",
        "double" => "%f"
    ];

    public function __construct(\DibiFluent $fluent)
    {
        $this->fluent = $fluent;
    }

    public function getModificators()
    {
        return $this->modificators;
    }

    public function setFilter(array $filter)
    {
        if ($filter) {
            $this->fluent->where("%and", $this->convertFilter($filter));
        }
    }

    public function setAssociations(array $associations)
    {
        $this->associations += $associations;
    }

    public function convertFilter(array $filter)
    {
        $result = [];

        if (Filter::isGroup($filter)) {
            // Filter group

            foreach ($filter as $modifier => $item) {
                $result[] = [
                    $modifier === Filter::_OR ? "%or" : "%and",
                    $this->convertFilter($item)
                ];
            }
        } else {
            // Filter item

            foreach ($filter as $name => $item) {

                foreach ($item as $operator => $value) {

                    // Convert data type definition to modificator
                    $type = gettype($value);
                    if ($type === "object") {
                        $type = get_class($value);
                    }
                    if (!isset($this->modificators[$type])) {
                        throw new \Exception("Unsupported value type " . $type . " given!");
                    }
                    $modificator = $this->modificators[$type];

                    if ($operator === Filter::START) {

                        $operator = "LIKE";
                        $modificator = "%like~";
                    } elseif ($operator === Filter::END) {

                        $operator = "LIKE";
                        $modificator = "%~like";
                    } elseif ($operator === Filter::CONTAIN) {

                        $operator = "LIKE";
                        $modificator = "%~like~";
                    }

                    if ($modificator === "%in") {

                        if ($operator === Filter::EQUAL) {
                            $operator = "IN";
                        } elseif ($operator === Filter::NOT) {
                            $operator = "NOT IN";
                        }
                    } elseif (in_array($modificator, ["%sN", "%b"], true)) {

                        if ($operator === Filter::EQUAL) {
                            $operator = "IS";
                        } elseif ($operator === Filter::NOT) {
                            $operator = "IS NOT";
                        }
                    }

                    if ($operator === Filter::NOT) {
                        $operator = "!=";
                    }

                    $result[] = [
                        "%n %sql " . $modificator,
                        $name,
                        $operator,
                        $value
                    ];
                }
            }
        }

        return $result;
    }

    public function getRaw()
    {
        return (string) $this->fluent;
    }

}