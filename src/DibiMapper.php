<?php

namespace UniMapper\Mapper;

use UniMapper\Exceptions\MapperException;

/**
 * Dibi mapper can be generally used to communicate between repository and
 * dibi database abstract layer.
 */
class DibiMapper extends \UniMapper\Mapper
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
        "array" => "%in"
    );

    /**
     * Constructor
     *
     * @param \DibiConnection $connection Database connection
     *
     * @return void
     */
    public function __construct(\DibiConnection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * Modify result value eg. convert DibiDateTime do Datetime etc.
     *
     * @param mixed $value Value
     *
     * @return mixed
     */
    protected function modifyResultValue($value)
    {
        if ($value instanceof \DibiDateTime) {
            return new \DateTime($value);
        }
        return $value;
    }

    /**
     * Get mapped conditions from query
     *
     * @param \DibiFluent    $fluent Dibi fluent
     * @param \UniMapper\Query $query  Query object
     *
     * @return \DibiFluent
     */
    protected function getConditions(\DibiFluent $fluent, \UniMapper\Query $query)
    {
        $properties = $this->getMapperProperties($query);
        foreach ($query->conditions as $condition) {

            $propertyName = $condition->getExpression();

            // Skip properties not related to this mapper
            if (!isset($properties[$propertyName])) {
                continue;
            }

            // Get column name
            $mappedPropertyName = $properties[$propertyName]->getMapping()->getName((string) $this);
            if ($mappedPropertyName) {
                $propertyName = $mappedPropertyName;
            }

            // Convert data type definition to dibi modificator
            $type = gettype($condition->getValue());
            if ($type === "object") {
                $type = get_class($type);
            }
            if (!isset($this->modificators[$type])) {
                throw new MapperException(
                    "Value type " . $type . " is not supported"
                );
            }

            // Get operator
            $operator = $condition->getOperator();
            if ($operator === "COMPARE") {
                if ($this->connection->getDriver() instanceof \DibiPostgreDriver) {
                    $operator = "ILIKE";
                } elseif ($this->connection->getDriver() instanceof \DibiMySqlDriver) {
                    $operator = "LIKE";
                }
            }

            // Add condition
            $fluent->where(
                "%n %sql " . $this->modificators[$type],
                $condition->getExpression(),
                $operator,
                $condition->getValue()
            );
        }
        return $fluent;
    }

    /**
     * Delete
     *
     * @param \UniMapper\Query\Delete $query Query
     *
     * @return mixed
     */
    public function delete(\UniMapper\Query\Delete $query)
    {
        // @todo this should pPrevent deleting all data, but it can be solved after primarProperty implement in better way
        if (count($query->conditions) === 0) {
            throw new MapperException("At least one condition must be specified!");
        }

        return $this->getConditions(
            $this->connection->delete($this->getResource($query)),
            $query
        )->execute();
    }

    /**
     * Find single record
     *
     * @param \UniMapper\Query\FindOne $query Query
     */
    public function findOne(\UniMapper\Query\FindOne $query)
    {
        throw new MapperException("Not implemented!");
    }

    /**
     * FindAll
     *
     * @param \UniMapper\Query\FindAll $query FindAll Query
     *
     * @return mixed
     */
    public function findAll(\UniMapper\Query\FindAll $query)
    {
        $selection = $this->getSelection($query);
        if (count($selection) === 0) {
            return false;
        }
        $fluent = $this->connection->select("[" . implode("],[", $selection) . "]");

        $fluent->from("%n", $this->getResource($query));
        $this->getConditions($fluent, $query);

        if ($query->limit > 0) {
            $fluent->limit("%i", $query->limit);
        }

        if ($query->offset > 0) {
            $fluent->offset("%i", $query->offset);
        }

        if (count($query->orders) > 0) {

            foreach ($query->orders as $order) {

                $fluent->orderBy($order->propertyName)
                    ->asc($order->asc)
                    ->desc($order->desc);
            }
        }

        $entityClass = $query->entityReflection->getName();
        $result = $fluent->fetchAll();
        if (count($result === 0)) {
            return false;
        }
        return $this->dataToCollection($result, new $entityClass, $query->entityReflection->getPrimaryProperty());
    }

    public function count(\UniMapper\Query\ICountable $query)
    {
        $fluent = $this->connection->select()->from("%n", $this->getResource($query));
        $this->getConditions($fluent, $query);
        return $fluent->count();
    }

    /**
     * Insert
     *
     * @param \UniMapper\Query\Insert $query Query
     *
     * @return mixed
     */
    public function insert(\UniMapper\Query\Insert $query)
    {
        $values = $this->entityToData($query->entity);
        if (empty($values)) {
            throw new MapperException("Entity has no mapped values!");
        }

        $this->connection->insert($this->getResource($query), $values)
            ->execute();

        return (integer) $this->connection->getInsertId();
    }

    /**
     * Update
     *
     * @param \UniMapper\Query\Update $query Query
     *
     * @return mixed
     */
    public function update(\UniMapper\Query\Update $query)
    {
        // @todo This can be solved after primarProperty implement
        if (count($query->conditions) === 0) {
            throw new MapperException("At least one condition must be specified!");
        }

        $fluent = $this->connection->update(
            $this->getResource($query),
            $this->entityToData($query->entity)
        );
        return $this->getConditions($fluent, $query)->execute();
    }

}