<?php

namespace UniMapper\Dibi;

class Query implements \UniMapper\Adapter\IQuery
{

    public $associations = [];

    /** @var \DibiFluent */
    public $fluent;

    public $resultCallback;

    /** @var array $modificators Dibi modificators */
    private $modificators = array(
        "boolean" => "%b",
        "integer" => "%i",
        "string" => "%s",
        "NULL" => "NULL",
        "DateTime" => "%t",
        "array" => "%in",
        "double" => "%f"
    );

    public function __construct(\DibiFluent $fluent)
    {
        $this->fluent = $fluent;
    }

    public function getModificators()
    {
        return $this->modificators;
    }

    public function setConditions(array $conditions)
    {
        $i = 0;
        foreach ($conditions as $condition) {

            list($joiner, $query, $modificators) = $this->convertCondition($condition);

            array_unshift($modificators, $query);

            if ($joiner === "AND" || $i === 0) {
                call_user_func_array([$this->fluent, "where"], $modificators);
            } else {
                call_user_func_array([$this->fluent, "or"], $modificators);
            }
            $i++;
        }
    }

    public function setAssociations(array $associations)
    {
        $this->associations += $associations;
    }

    public function convertCondition(array $condition)
    {
        if (is_array($condition[0])) {
            // Nested conditions

            list($nestedConditions, $joiner) = $condition;

            $i = 0;
            $query = "";
            $modificators = array();
            foreach ($nestedConditions as $nestedCondition) {
                list($conditionJoiner, $conditionQuery, $conditionModificators) = $this->convertCondition($nestedCondition);
                if ($i > 0) {
                    $query .= " " . $conditionJoiner . " ";
                }
                $query .= $conditionQuery;
                $modificators = array_merge($modificators, $conditionModificators);
                $i++;
            }
            return array(
                $joiner,
                "(" . $query . ")",
                $modificators
            );
        } else {
            // Simple condition

            list($columnName, $operator, $value, $joiner) = $condition;

            // Convert data type definition to dibi modificator
            $type = gettype($value);
            if ($type === "object") {
                $type = get_class($value);
            }
            if (!isset($this->modificators[$type])) {
                throw new \Exception("Unsupported value type " . $type . " given!");
            }

            // Get operator
            if ($operator === "COMPARE") {
                if ($this->fluent->connection->getDriver() instanceof \DibiPostgreDriver) {
                    $operator = "ILIKE";
                } elseif ($this->fluent->connection->getDriver() instanceof \DibiMySqlDriver) {
                    $operator = "LIKE";
                }
            }

            return array(
                $joiner,
                "%n %sql " . $this->modificators[$type],
                array(
                    $columnName,
                    $operator,
                    $value
                )
            );
        }
    }

    public function getRaw()
    {
        return (string) $this->fluent;
    }

}
