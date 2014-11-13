<?php

namespace UniMapper\Dibi\Adapter;

use UniMapper\Reflection;

class Mapper extends \UniMapper\Adapter\Mapper
{

    public function mapValue(Reflection\Entity\Property $property, $data)
    {
        if ($data instanceof \DibiDateTime) {
            return new \DateTime($data);
        }
        return parent::mapValue($property, $data);
    }

}