<?php

$loader = @include __DIR__ . '/../vendor/autoload.php';
if (!$loader) {
    echo 'Install Nette Tester using `composer update --dev`';
    exit(1);
}

Tester\Environment::setup();

date_default_timezone_set('Europe/Prague');

try {
    $config = Tester\Environment::loadData() + ['user' => null, 'password' => null];
} catch (Exception $e) {
    Tester\Environment::skip($e->getMessage());
}