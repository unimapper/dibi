<?php

namespace UniMapper\Dibi;

use UniMapper\Reflection;

class Mapping extends \UniMapper\Mapping
{

    public function mapValue(Reflection\Entity\Property $property, $data)
    {
        if ($data instanceof \DibiDateTime) {
            return new \DateTime($data);
        }
        return parent::mapValue($property, $data);
    }

}