<?php

require_once __DIR__ . '/../vendor/autoload.php';

use decMuc\PDOdb\PDOdb;


$isCli = (php_sapi_name() === 'cli');
$nl = $isCli ? "\n" : "<br>";


// ⚠️ Use your own DB connection credentials here
$db = new PDOdb([
    'host' => '127.0.0.1',
    'username' => 'root',
    'password' => '',
    'db'       => 'testdb',
    'charset'  => 'utf8mb4'
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
// ✅ Tabelle anlegen (falls nicht vorhanden)
$res = $db->rawQuery("
    CREATE TABLE IF NOT EXISTS `test_products_benchmark` (
        `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `name` VARCHAR(255) NOT NULL,
        `category_id` INT UNSIGNED NOT NULL,
        `supplier_id` INT UNSIGNED NOT NULL,
        `price` DECIMAL(10,2) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
");

$db->rawQuery("TRUNCATE TABLE `test_products_benchmark`");

// 🔁 Dummy-Data
$rowsA = [];
for ($i = 1; $i <= 1000; $i++) {
    $rowsA[] = [
        'name'        => 'Product ' . $i,
        'category_id' => rand(1, 5),
        'supplier_id' => rand(1, 5),
        'price'       => rand(100, 999) + 0.99
    ];
}

$rowsB = [];
for ($i = 1001; $i <= 2000; $i++) {
    $rowsB[] = [
        'name'        => 'Product ' . $i,
        'category_id' => rand(1, 5),
        'supplier_id' => rand(1, 5),
        'price'       => rand(100, 999) + 0.99
    ];
}

// 🚀 insertMulti Benchmark
$start = microtime(true);
$ids = $db->insertMulti('test_products_benchmark', $rowsA);
$multiDuration = round(microtime(true) - $start, 4);

// 🚀 insertBulk Benchmark
$start = microtime(true);
$affected = $db->insertBulk('test_products_benchmark', $rowsB);
$bulkDuration = round(microtime(true) - $start, 4);

// 📤 Ergebnis ausgeben
echo "insertMulti(): " . count((array)$ids) . " rows in {$multiDuration}s" . $nl;
echo "insertBulks():  {$affected} rows in {$bulkDuration}s" . $nl;

$result = $db->get('test_products_benchmark');

echo "Total Rows: ".count($result) . $nl;

