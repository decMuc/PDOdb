<?php

require_once __DIR__ . '/../vendor/autoload.php';

use decoMuc\PDOdb\PDOdb;

// Auto linebreak detection (CLI vs. browser)
$isCli = (php_sapi_name() === 'cli');
$nl = $isCli ? "\n" : "<br>";

// DB connection (adjust as needed)
$db = new PDOdb([
    'host' => '127.0.0.1',
    'username' => 'root',
    'password' => '',
    'database' => 'testdb',
    'charset' => 'utf8mb4',
]);

// Recreate tables for testing
$db->rawQuery("DROP TABLE IF EXISTS test_products");
$db->rawQuery("DROP TABLE IF EXISTS test_categories");
$db->rawQuery("DROP TABLE IF EXISTS test_suppliers");

$db->rawQuery("
    CREATE TABLE test_categories (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(50) NOT NULL
    ) ENGINE=InnoDB
");

$db->rawQuery("
    CREATE TABLE test_suppliers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        company VARCHAR(100) NOT NULL
    ) ENGINE=InnoDB
");

$db->rawQuery("
    CREATE TABLE test_products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        category_id INT,
        supplier_id INT,
        FOREIGN KEY (category_id) REFERENCES test_categories(id),
        FOREIGN KEY (supplier_id) REFERENCES test_suppliers(id)
    ) ENGINE=InnoDB
");

// Insert sample data
$db->insertMulti('test_categories', [
    ['name' => 'Electronics'],
    ['name' => 'Books'],
    ['name' => 'Toys']
]);

$db->insertMulti('test_suppliers', [
    ['company' => 'Acme Ltd.'],
    ['company' => 'Globex Corp.']
]);

$db->insertMulti('test_products', [
    ['name' => 'Smartphone', 'price' => 499.99, 'category_id' => 1, 'supplier_id' => 1],
    ['name' => 'Tablet', 'price' => 299.00, 'category_id' => 1, 'supplier_id' => 2],
    ['name' => 'Book A', 'price' => 14.95, 'category_id' => 2, 'supplier_id' => null],
    ['name' => 'Drone', 'price' => 799.00, 'category_id' => 1, 'supplier_id' => 1],
    ['name' => 'Puzzle', 'price' => 9.99, 'category_id' => 3, 'supplier_id' => null]
]);

echo $nl . "=== Tables and sample data created ===" . $nl;

// INNER JOIN: products with category (price > 100 EUR)
$db->join("test_categories c", "p.category_id = c.id", "INNER");
$db->where("p.price", 100, ">");
$rows = $db->get("test_products p", null, "p.*, c.name AS category_name");

echo $nl . "=== INNER JOIN (Products > 100 EUR with category) ===" . $nl;
print_r($rows);

// LEFT JOIN: products with supplier (even if supplier_id is NULL)
$db->join("test_suppliers s", "p.supplier_id = s.id", "LEFT");
$rows = $db->get("test_products p", null, "p.name, s.company");

echo $nl . "=== LEFT JOIN (Products with optional supplier) ===" . $nl;
print_r($rows);

// JOIN with setMapKey (map by product ID)
$db->join("test_suppliers s", "p.supplier_id = s.id", "LEFT");
$db->setMapKey("p.id");
$mapped = $db->get("test_products p", null, "p.*, s.company AS supplier");

echo $nl . "=== JOIN using setMapKey(p.id) ===" . $nl;
print_r($mapped);