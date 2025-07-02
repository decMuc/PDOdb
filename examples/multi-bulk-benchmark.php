<?php

require_once __DIR__ . '/../vendor/autoload.php';

use decoMuc\PDOdb\PDOdb;


$isCli = (php_sapi_name() === 'cli');
$nl = $isCli ? "\n" : "<br>";


// ⚠️ Use your own DB connection credentials here
$db = new PDOdb([
    'host' => '127.0.0.1',
    'username' => 'root',
    'password' => '',
    'database' => 'testdb',
    'charset' => 'utf8mb4'
]);

// Dummy data generator
$rows = [];
for ($i = 1; $i <= 1000; $i++) {
    $rows[] = [
        'name' => 'Product ' . $i,
        'category_id' => rand(1, 5),
        'supplier_id' => rand(1, 5),
        'price' => rand(100, 999) + 0.99
    ];
}

// insertMulti
$start = microtime(true);
$ids = $db->insertMulti('test_products_benchmark', $rows);
$multiDuration = round(microtime(true) - $start, 4);

// insertBulk
$start = microtime(true);
$affected = $db->insertBulk('test_products_benchmark', $rows);
$bulkDuration = round(microtime(true) - $start, 4);

// Output result
echo "insertMulti(): " . count((array)$ids) . " rows in {$multiDuration}s". $nl;
echo "insertBulk():  {$affected} rows in {$bulkDuration}s". $nl;

