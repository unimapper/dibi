<?php

namespace UniMapper\Dibi;

use UniMapper\Exceptions\AdapterException,
    UniMapper\Reflection\Entity\Property\Association\BelongsToMany,
    UniMapper\Reflection\Entity\Property\Association\HasMany;

class Adapter extends \UniMapper\Adapter
{

    /** @var \DibiConnection $connection Dibi connection */
    protected $connection;

    /** @var array $modificators Dibi modificators */
    protected $modificators = array(
        "boolean" => "%b",
        "integer" => "%i",
        "string" => "%s",
        "NULL" => "NULL",
        "DateTime" => "%t",
        "array" => "%in",
        "double" => "%f"
    );

    public function __construct($name, \DibiConnection $connection)
    {
        parent::__construct($name, new Mapping);
        $this->connection = $connection;
    }

    /**
     * Raw query
     *
     * @return \DibiConnection
     */
    public function getConnection()
    {
        return $this->connection;
    }

    protected function setConditions($fluent, array $conditions)
    {
        $i = 0;
        foreach ($conditions as $condition) {

            list($joiner, $query, $modificators) = $this->convertCondition($condition);

            array_unshift($modificators, $query);

            if ($joiner === "AND" || $i === 0) {
                call_user_func_array(array($fluent, "where"), $modificators);
            } else {
                call_user_func_array(array($fluent, "or"), $modificators);
            }
            $i++;
        }
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
                $type = get_class($type);
            }
            if (!isset($this->modificators[$type])) {
                throw new AdapterException("Unsupported value type " . $type . " given!");
            }

            // Get operator
            if ($operator === "COMPARE") {
                if ($this->connection->getDriver() instanceof \DibiPostgreDriver) {
                    $operator = "ILIKE";
                } elseif ($this->connection->getDriver() instanceof \DibiMySqlDriver) {
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

    /**
     * Delete record by some conditions
     *
     * @param string $resource
     * @param array  $conditions
     */
    public function delete($resource, $conditions)
    {
        $fluent = $this->connection->delete($resource);
        $this->setConditions($fluent, $conditions);
        $fluent->execute();
    }

    /**
     * Find single record identified by primary value
     *
     * @param string $resource
     * @param mixed  $primaryName
     * @param mixed  $primaryValue
     * @param array  $associations
     *
     * @return mixed
     */
    public function findOne($resource, $primaryName, $primaryValue, array $associations = [])
    {
        return $this->connection->select("*")
            ->from("%n", $resource)
            ->where("%n = %s", $primaryName, $primaryValue) // @todo
            ->fetch();
    }

    /**
     * Find records
     *
     * @param string  $resource
     * @param array   $selection
     * @param array   $conditions
     * @param array   $orderBy
     * @param integer $limit
     * @param integer $offset
     * @param array   $associations
     *
     * @return array|false
     */
    public function find($resource, $selection = null, $conditions = null, $orderBy = null, $limit = 0, $offset = 0, array $associations = [])
    {
        $fluent = $this->connection->select("[" . implode("],[", $selection) . "]")->from("%n", $resource);

        if (!empty($limit)) {
            $fluent->limit("%i", $limit);
        }

        if (!empty($offset)) {
            $fluent->offset("%i", $offset);
        }

        $this->setConditions($fluent, $conditions);

        foreach ($orderBy as $name => $direction) {
            $fluent->orderBy($name)->{$direction}();
        }

        $result = $fluent->fetchAll(null);
        if (count($result) === 0) {
            return false;
        }

        // Associations
        $associated = [];
        foreach ($associations as $propertyName => $association) {

            $primaryKeys = [];
            foreach ($result as $row) {
                $primaryKeys[] = $row->{$association->getPrimaryKey()};
            }

            if ($association instanceof BelongsToMany) {
                $associated[$propertyName] = $this->_belongsToMany($association, $primaryKeys);
            } elseif ($association instanceof HasMany) {
                $associated[$propertyName] = $this->_hasMany($association, $primaryKeys);
            } else {
                throw new AdapterException("Unsupported association " . get_class($association) . "!");
            }
        }

        foreach ($result as $index => $item) {

            foreach ($associated as $propertyName => $associatedResult) {

                $primaryValue = $item->{$association->getPrimaryKey()}; // @todo potencial future bug, association wrong?
                if (isset($associatedResult[$primaryValue])) {
                    $item[$propertyName] = $associatedResult[$primaryValue];
                }
            }
        }

        return $result;
    }

    private function _belongsToMany(BelongsToMany $association, array $primaryKeys)
    {
        return $this->connection->select("*")
            ->from("%n", $association->getTargetResource())
            ->where("%n IN %l", $association->getForeignKey(), $primaryKeys)
            ->fetchAssoc($association->getForeignKey() . ",#");
    }

    private function _hasMany(HasMany $association, array $primaryKeys)
    {
        $joinResult = $this->connection->select("%n,%n", $association->getJoinKey(), $association->getReferenceKey())
            ->from("%n", $association->getJoinResource())
            ->where("%n IN %l", $association->getJoinKey(), $primaryKeys)
            ->fetchAssoc($association->getReferenceKey() . "," . $association->getJoinKey());

        $targetResult = $this->connection->select("*")
            ->from("%n", $association->getTargetResource())
            ->where("%n IN %l", $association->getForeignKey(), array_keys($joinResult))
            ->fetchAssoc($association->getForeignKey());

        $result = [];
        foreach ($joinResult as $targetKey => $join) {

            foreach ($join as $originKey => $data) {
                $result[$originKey][] = $targetResult[$targetKey];
            }
        }

        return $result;
    }

    public function count($resource, $conditions)
    {
        $fluent = $this->connection->select("*")->from("%n", $resource);
        $this->setConditions($fluent, $conditions);
        return $fluent->count();
    }

    /**
     * Insert
     *
     * @param string $resource
     * @param array  $values
     *
     * @return mixed Primary value
     */
    public function insert($resource, array $values)
    {
        $this->connection->insert($resource, $values)->execute();
        return $this->connection->getInsertId();
    }

    /**
     * Update data by set of conditions
     *
     * @param string $resource
     * @param array  $values
     * @param array  $conditions
     */
    public function update($resource, array $values, $conditions = null)
    {
        $fluent = $this->connection->update($resource, $values);
        $this->setConditions($fluent, $conditions);
        $fluent->execute();
    }

    /**
     * Update single record
     *
     * @param string $resource
     * @param string $primaryName
     * @param mixed  $primaryValue
     * @param array  $values
     *
     * @return mixed Primary value
     */
    public function updateOne($resource, $primaryName, $primaryValue, array $values)
    {
        $type = gettype($primaryValue);
        if ($type === "object") {
            $type = get_class($type);
        }
        $this->connection->update($resource, $values)
            ->where("%n = " . $this->modificators[$type], $primaryName, $primaryValue)
            ->execute();
    }

}