<?php
require_once __DIR__ . '/../vendor/autoload.php';

use decMuc\PDOdb\PDOdb;

$isCli = (php_sapi_name() === 'cli');
$nl = $isCli ? "\n" : "<br>";

// ⚠️ Adjust your DB connection credentials as needed
$db = new PDOdb([
    'host'     => '127.0.0.1',
    'username' => 'root',
    'password' => '',
    'db'       => 'testdb',
    'charset'  => 'utf8mb4'
]);

// Drop old tables if they exist
$db->rawQuery("DROP TABLE IF EXISTS test_orders");
$db->rawQuery("DROP TABLE IF EXISTS test_products");
$db->rawQuery("DROP TABLE IF EXISTS test_tx_log");

// Create product table
$db->rawQuery("
    CREATE TABLE test_products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        stock INT NOT NULL DEFAULT 0,
        price DECIMAL(10,2) NOT NULL,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
");

// Create orders table
$db->rawQuery("
    CREATE TABLE test_orders (
        id INT AUTO_INCREMENT PRIMARY KEY,
        product_id INT NOT NULL,
        quantity INT NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES test_products(id)
    )
");

// Create log table
$db->rawQuery("
    CREATE TABLE test_tx_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        note VARCHAR(255) NOT NULL,
        counter INT NOT NULL DEFAULT 0,
        last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
");

echo "{$nl}=== Tables created successfully ==={$nl}";

// === TEST 1: Basic INSERTs ===
$db->insert('test_products', ['name' => 'Widget A', 'stock' => 50, 'price' => 9.99]);
$db->insert('test_products', ['name' => 'Widget B', 'stock' => 25, 'price' => 19.95]);

$db->insert('test_tx_log', ['note' => 'Start', 'counter' => 1]);

echo "{$nl}=== INSERT complete ==={$nl}";

// === TEST 2: WHERE + OR ===
$db->where('name', 'Widget A');
$db->orWhere('stock', 25);
$res = $db->get('test_products');

echo "{$nl}=== WHERE / ORWHERE ==={$nl}";
print_r($res);

// === TEST 3: Lock Table + Update ===
$db->lock('test_products');
$db->where('name', 'Widget A')->update('test_products', ['stock' => 999]);
$db->unlock();

echo "{$nl}=== LOCK / UPDATE successful ==={$nl}";

// === TEST 4: Transaction with Rollback ===
$db->startTransaction();

$db->insert('test_orders', ['product_id' => 1, 'quantity' => 5]);
$db->insert('test_tx_log', ['note' => 'Intermediate', 'counter' => 2]);

// Simulate failure (e.g. invalid foreign key)
$fail = $db->insert('test_orders', ['product_id' => 9999, 'quantity' => 1]);

if (!$fail) {
    $db->rollback();
    echo "{$nl}=== ROLLBACK triggered ==={$nl}";
} else {
    $db->commit();
    echo "{$nl}=== TRANSACTION committed ==={$nl}";
}

// === TEST 5: ON UPDATE timestamp ===
$db->where('note', 'Start')->update('test_tx_log', ['counter' => 42]);

$res = $db->map('id')->get('test_tx_log');
echo "{$nl}=== ON UPDATE check ==={$nl}";
print_r($res);