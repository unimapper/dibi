<?php

namespace UniMapper\Dibi\Adapter;

use UniMapper\Reflection;

class Mapping extends \UniMapper\Adapter\Mapping
{

    public function mapValue(Reflection\Entity\Property $property, $value)
    {
        if ($value instanceof \DibiDateTime) {
            return new \DateTime($value);
        }
        return $value;
    }

}