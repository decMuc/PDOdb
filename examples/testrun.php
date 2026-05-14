<?php

/**
 * PDOdb Test Script – HTML Edition mit Zeitmessung
 */

$scriptStart = microtime(true);

// ============================================================
// == KONFIGURATION – bitte anpassen ==
// ============================================================

define('DB_HOST',     'localhost');
define('DB_USER',     'pdo');
define('DB_PASS',     '******************');
define('DB_NAME',     'pdo');
define('DB_PORT',     3306);
define('DB_CHARSET',  'utf8mb4');
define('DB_PREFIX',   'tst_');

// ============================================================
// == AUTOLOAD (ohne Composer) ==
// ============================================================

require_once __DIR__ . '/decMuc/PDOdb/PDOdb.php';

use decMuc\PDOdb\PDOdb;

// ============================================================
// == TEST FRAMEWORK ==
// ============================================================

$passed  = 0;
$failed  = 0;
$results = [];

function section(string $name): void
{
    global $results;
    $results[] = ['type' => 'section', 'name' => $name];
}

function test(string $name, callable $fn): void
{
    global $passed, $failed, $results;

    $start = microtime(true);
    try {
        $result = $fn();
        $ms = round((microtime(true) - $start) * 1000, 2);

        if ($result === true || $result === null) {
            $passed++;
            $results[] = ['type' => 'test', 'status' => 'PASS', 'name' => $name, 'info' => '', 'ms' => $ms];
        } else {
            $failed++;
            $results[] = ['type' => 'test', 'status' => 'FAIL', 'name' => $name, 'info' => 'Returned: ' . var_export($result, true), 'ms' => $ms];
        }
    } catch (\Throwable $e) {
        $ms = round((microtime(true) - $start) * 1000, 2);
        $failed++;
        $results[] = ['type' => 'test', 'status' => 'ERROR', 'name' => $name, 'info' => $e->getMessage(), 'ms' => $ms];
    }
}

// ============================================================
// == DATENBANK + TABELLEN ANLEGEN ==
// ============================================================

$setupError = null;
try {
    $setupPdo = new \PDO(
        "mysql:host=" . DB_HOST . ";port=" . DB_PORT . ";charset=" . DB_CHARSET,
        DB_USER,
        DB_PASS,
        [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION]
    );

    $setupPdo->exec("CREATE DATABASE IF NOT EXISTS `" . DB_NAME . "` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
    $setupPdo->exec("USE `" . DB_NAME . "`");

    $setupPdo->exec("DROP TABLE IF EXISTS `" . DB_PREFIX . "order_items`");
    $setupPdo->exec("DROP TABLE IF EXISTS `" . DB_PREFIX . "orders`");
    $setupPdo->exec("DROP TABLE IF EXISTS `" . DB_PREFIX . "users`");

    $setupPdo->exec("
        CREATE TABLE `" . DB_PREFIX . "users` (
            `id`         INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            `name`       VARCHAR(100) NOT NULL,
            `email`      VARCHAR(150) NOT NULL UNIQUE,
            `role`       ENUM('admin','editor','viewer') NOT NULL DEFAULT 'viewer',
            `active`     TINYINT(1) NOT NULL DEFAULT 1,
            `score`      DECIMAL(8,2) DEFAULT NULL,
            `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    ");

    $setupPdo->exec("
        CREATE TABLE `" . DB_PREFIX . "orders` (
            `id`         INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            `user_id`    INT UNSIGNED NOT NULL,
            `status`     ENUM('pending','processing','shipped','cancelled') NOT NULL DEFAULT 'pending',
            `total`      DECIMAL(10,2) NOT NULL DEFAULT 0.00,
            `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    ");

    $setupPdo->exec("
        CREATE TABLE `" . DB_PREFIX . "order_items` (
            `id`         INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            `order_id`   INT UNSIGNED NOT NULL,
            `product`    VARCHAR(100) NOT NULL,
            `qty`        INT NOT NULL DEFAULT 1,
            `price`      DECIMAL(8,2) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    ");

    $setupPdo->exec("
        INSERT INTO `" . DB_PREFIX . "users` (`name`, `email`, `role`, `active`, `score`) VALUES
        ('Alice Admin',   'alice@example.com',   'admin',  1, 98.50),
        ('Bob Editor',    'bob@example.com',     'editor', 1, 74.00),
        ('Charlie View',  'charlie@example.com', 'viewer', 1, 55.25),
        ('Dana Inactive', 'dana@example.com',    'viewer', 0, 10.00),
        ('Eve Editor',    'eve@example.com',     'editor', 1, 88.75)
    ");

    $setupPdo->exec("
        INSERT INTO `" . DB_PREFIX . "orders` (`user_id`, `status`, `total`) VALUES
        (1, 'shipped',    120.00),
        (1, 'pending',     45.50),
        (2, 'processing',  89.99),
        (3, 'cancelled',   15.00),
        (5, 'shipped',    210.00),
        (5, 'pending',     33.00)
    ");

    $setupPdo->exec("
        INSERT INTO `" . DB_PREFIX . "order_items` (`order_id`, `product`, `qty`, `price`) VALUES
        (1, 'Laptop Stand',   1,  49.99),
        (1, 'USB Hub',        2,  19.99),
        (1, 'Mouse Pad',      1,  29.99),
        (2, 'Keyboard',       1,  45.50),
        (3, 'Monitor',        1,  89.99),
        (4, 'HDMI Cable',     1,  15.00),
        (5, 'Webcam',         1,  79.99),
        (5, 'Ring Light',     2,  64.99),
        (6, 'Phone Stand',    1,  33.00)
    ");

    $setupPdo = null;

} catch (\Throwable $e) {
    $setupError = $e->getMessage();
}

// ============================================================
// == PDOdb INSTANZ + TESTS ==
// ============================================================

if (!$setupError) {
    $db = new PDOdb([
        'host'     => DB_HOST,
        'username' => DB_USER,
        'password' => DB_PASS,
        'db'       => DB_NAME,
        'port'     => DB_PORT,
        'charset'  => DB_CHARSET,
        'prefix'   => DB_PREFIX,
    ]);

    section('BASIC SELECT');

    test('get() alle User', function () use ($db) {
        $rows = $db->get('users');
        return is_array($rows) && count($rows) === 5;
    });

    test('getOne() einzelner User', function () use ($db) {
        $row = $db->where('email', 'alice@example.com')->getOne('users');
        return is_array($row) && $row['name'] === 'Alice Admin';
    });

    test('getValue() einzelner Wert', function () use ($db) {
        $name = $db->where('id', 1)->getValue('users', 'name');
        return $name === 'Alice Admin';
    });

    test('getValue() mehrere Werte', function () use ($db) {
        $names = $db->getValue('users', 'name', null);
        return is_array($names) && count($names) === 5;
    });

    test('get() mit LIMIT', function () use ($db) {
        $rows = $db->get('users', 3);
        return is_array($rows) && count($rows) === 3;
    });

    test('get() mit OFFSET + LIMIT', function () use ($db) {
        $rows = $db->get('users', [2, 2]);
        return is_array($rows) && count($rows) === 2;
    });

    section('WHERE CLAUSES');

    test('where() mit Operator', function () use ($db) {
        $rows = $db->where('score', 80, '>')->get('users');
        return is_array($rows) && count($rows) === 2;
    });

    test('whereIn()', function () use ($db) {
        $rows = $db->whereIn('role', ['admin', 'editor'])->get('users');
        return is_array($rows) && count($rows) === 3;
    });

    test('whereNotIn()', function () use ($db) {
        $rows = $db->whereNotIn('role', ['admin'])->get('users');
        return is_array($rows) && count($rows) === 4;
    });

    test('whereIsNull()', function () use ($db) {
        $rows = $db->whereIsNull('score')->get('users');
        return is_array($rows) && count($rows) === 0;
    });

    test('whereIsNotNull()', function () use ($db) {
        $rows = $db->whereIsNotNull('score')->get('users');
        return is_array($rows) && count($rows) === 5;
    });

    test('whereBool() aktive User', function () use ($db) {
        $rows = $db->whereBool('active', true)->get('users');
        return is_array($rows) && count($rows) === 4;
    });

    test('whereInt()', function () use ($db) {
        $row = $db->whereInt('id', 2)->getOne('users');
        return is_array($row) && $row['email'] === 'bob@example.com';
    });

    test('whereString()', function () use ($db) {
        $row = $db->whereString('email', 'eve@example.com')->getOne('users');
        return is_array($row) && $row['name'] === 'Eve Editor';
    });

    test('orWhere()', function () use ($db) {
        $rows = $db->where('role', 'admin')->orWhere('role', 'editor')->get('users');
        return is_array($rows) && count($rows) === 3;
    });

    test('whereDate()', function () use ($db) {
        $today = date('Y-m-d');
        $rows  = $db->whereDate('created_at', $today, '>=')->get('users');
        return is_array($rows) && count($rows) === 5;
    });

    section('ORDER / GROUP / LIMIT');

    test('orderBy() ASC', function () use ($db) {
        $rows = $db->orderBy('score', 'ASC')->get('users');
        return is_array($rows) && $rows[0]['name'] === 'Dana Inactive';
    });

    test('orderBy() DESC', function () use ($db) {
        $rows = $db->orderBy('score', 'DESC')->get('users');
        return is_array($rows) && $rows[0]['name'] === 'Alice Admin';
    });

    test('groupBy() + COUNT', function () use ($db) {
        $rows = $db->groupBy('role')->get('users', null, ['role', 'COUNT(*) AS cnt']);
        return is_array($rows) && count($rows) === 3;
    });

    section('INSERT / UPDATE / DELETE');

    test('insert() neuer User', function () use ($db) {
        $id = $db->insert('users', [
            'name'  => 'Test User',
            'email' => 'test@example.com',
            'role'  => 'viewer',
            'score' => 42.00,
        ]);
        return is_int($id) && $id > 0;
    });

    test('insert() lastInsertId korrekt', function () use ($db) {
        $id  = $db->insert('users', ['name' => 'Test User 2', 'email' => 'test2@example.com', 'role' => 'viewer']);
        $row = $db->whereInt('id', $id)->getOne('users');
        return $row['email'] === 'test2@example.com';
    });

    test('update()', function () use ($db) {
        return $db->whereString('email', 'test@example.com')->update('users', ['score' => 99.99]) === true;
    });

    test('update() – Wert korrekt gespeichert', function () use ($db) {
        $row = $db->whereString('email', 'test@example.com')->getOne('users');
        return (float)$row['score'] === 99.99;
    });

    test('inc() / dec()', function () use ($db) {
        $db->whereString('email', 'test@example.com')->update('users', ['score' => $db->inc(0.01)]);
        $row = $db->whereString('email', 'test@example.com')->getOne('users');
        return (float)$row['score'] === 100.00;
    });

    test('delete()', function () use ($db) {
        return $db->whereString('email', 'test2@example.com')->delete('users') === true;
    });

    test('delete() – wirklich weg', function () use ($db) {
        return $db->whereString('email', 'test2@example.com')->getOne('users') === false;
    });

    section('BULK INSERT');

    test('insertBulk()', function () use ($db) {
        $count = $db->insertBulk('order_items', [
            ['order_id' => 1, 'product' => 'Bulk Item A', 'qty' => 1, 'price' => 9.99],
            ['order_id' => 1, 'product' => 'Bulk Item B', 'qty' => 2, 'price' => 4.99],
            ['order_id' => 1, 'product' => 'Bulk Item C', 'qty' => 3, 'price' => 2.99],
        ]);
        return $count === 3;
    });

    test('insertMulti()', function () use ($db) {
        $ids = $db->insertMulti('order_items', [
            ['order_id' => 2, 'product' => 'Multi Item A', 'qty' => 1, 'price' => 19.99],
            ['order_id' => 2, 'product' => 'Multi Item B', 'qty' => 1, 'price' => 29.99],
        ]);
        return is_array($ids) && count($ids) === 2;
    });

    section('JOINS');

    test('join() LEFT – User mit Orders', function () use ($db) {
        $rows = $db
            ->join('orders o', 'o.user_id = u.id', 'LEFT')
            ->whereInt('u.id', 1)
            ->get('users u', null, ['u.name', 'o.status', 'o.total']);
        return is_array($rows) && count($rows) === 2;
    });

    test('join() INNER – nur User mit Orders', function () use ($db) {
        $rows = $db
            ->join('orders o', 'o.user_id = u.id', 'INNER')
            ->get('users u', null, ['u.id', 'u.name', 'o.total']);
        return is_array($rows) && count($rows) >= 4;
    });

    test('join() mit joinWhere()', function () use ($db) {
        $rows = $db
            ->join('orders o', 'o.user_id = u.id', 'LEFT')
            ->joinWhere('orders o', 'o.status', 'shipped')
            ->whereInt('u.id', 1)
            ->get('users u', null, ['u.name', 'o.status']);
        return is_array($rows);
    });

    section('SUBQUERY');

    test('subQuery() in WHERE', function () use ($db) {
        $sub = $db->subQuery();
        $sub->where('status', 'shipped')->get('tst_orders', null, 'user_id');
        $rows = $db->where('id', $sub, 'IN')->get('users');
        return is_array($rows) && count($rows) >= 1;
    });

    test('subQuery() in INSERT', function () use ($db) {
        $sub = $db->subQuery();
        $sub->where('email', 'alice@example.com')->get('tst_users', 1, 'id');
        $id = $db->insert('orders', [
            'user_id' => $sub,
            'status'  => 'pending',
            'total'   => 55.00,
        ]);
        return is_int($id) && $id > 0;
    });

    section('RAW QUERIES');

    test('rawQuery() SELECT', function () use ($db) {
        $rows = $db->rawQuery("SELECT * FROM tst_users WHERE active = ?", [1]);
        return is_array($rows) && count($rows) >= 4;
    });

    test('rawQuery() INSERT', function () use ($db) {
        $id = $db->rawQuery("INSERT INTO tst_users (name, email, role) VALUES (?, ?, ?)", ['Raw User', 'raw@example.com', 'viewer']);
        return is_int($id) || is_string($id);
    });

    test('rawQueryOne()', function () use ($db) {
        $row = $db->rawQueryOne("SELECT * FROM tst_users WHERE email = ?", ['alice@example.com']);
        return is_array($row) && $row['name'] === 'Alice Admin';
    });

    test('rawQueryValue()', function () use ($db) {
        $name = $db->rawQueryValue("SELECT name FROM tst_users WHERE email = ? LIMIT 1", ['alice@example.com']);
        return $name === 'Alice Admin';
    });

    section('PAGINATION');

    test('paginate() Seite 1', function () use ($db) {
        $db->setPageLimit(2);
        $rows = $db->paginate('users', 1);
        return is_array($rows) && count($rows) === 2;
    });

    test('paginate() totalCount', function () use ($db) {
        $db->setPageLimit(2);
        $db->paginate('users', 1);
        return $db->getTotalCount() >= 5;
    });

    test('paginate() totalPages', function () use ($db) {
        $db->setPageLimit(2);
        $db->paginate('users', 1);
        return $db->getTotalPages() >= 3;
    });

    section('TRANSACTIONS');

    test('Transaction commit', function () use ($db) {
        $db->startTransaction();
        $db->insert('orders', ['user_id' => 1, 'status' => 'pending', 'total' => 1.00]);
        $db->commit();
        $rows = $db->where('total', 1.00)->get('orders');
        return is_array($rows) && count($rows) >= 1;
    });

    test('Transaction rollback', function () use ($db) {
        $before = count($db->get('orders'));
        $db->startTransaction();
        $db->insert('orders', ['user_id' => 1, 'status' => 'pending', 'total' => 999.99]);
        $db->rollback();
        $after = count($db->get('orders'));
        return $after === $before;
    });

    section('OUTPUT MODES');

    test('jsonBuilder()', function () use ($db) {
        $json    = $db->jsonBuilder()->where('id', 1)->get('users');
        $decoded = json_decode($json, true);
        return is_array($decoded) && count($decoded) === 1;
    });

    test('objectBuilder()', function () use ($db) {
        $rows = $db->objectBuilder()->where('id', 1)->get('users');
        return is_array($rows) && $rows[0] instanceof \stdClass;
    });

    test('map() / setReturnKey()', function () use ($db) {
        $rows = $db->map('id')->get('users');
        return is_array($rows) && array_key_exists(1, $rows);
    });

    section('SPECIAL HELPERS');

    test('has() – existiert', function () use ($db) {
        return $db->where('email', 'alice@example.com')->has('users') === true;
    });

    test('has() – existiert nicht', function () use ($db) {
        return $db->where('email', 'nobody@example.com')->has('users') === false;
    });

    test('tableExists() – existiert', function () use ($db) {
        return $db->tableExists('users') === true;
    });

    test('tableExists() – existiert nicht', function () use ($db) {
        return $db->tableExists('nonexistent_table') === false;
    });

    test('getEnumValues()', function () use ($db) {
        $vals = $db->getEnumValues('users', 'role');
        return in_array('admin', $vals) && in_array('editor', $vals) && in_array('viewer', $vals);
    });

    test('onDuplicate()', function () use ($db) {
        $db->insert('users', ['name' => 'Dup User', 'email' => 'dup@example.com', 'role' => 'viewer', 'score' => 10.00]);
        $db->onDuplicate(['score'])->insert('users', ['name' => 'Dup User', 'email' => 'dup@example.com', 'role' => 'viewer', 'score' => 50.00]);
        $row = $db->whereString('email', 'dup@example.com')->getOne('users');
        return (float)$row['score'] === 50.00;
    });

    section('COPY / CLONE');

    test('copy() – unabhängige Kopie', function () use ($db) {
        $db->where('active', 1);
        $copy = $db->copy();
        $copy->where('role', 'admin');

        $orig   = $db->get('users');
        $copied = $copy->get('users');

        // Copy hat extra role-Filter → muss weniger Ergebnisse haben
        return is_array($orig)
            && is_array($copied)
            && count($copied) < count($orig)
            && count($copied) === 1; // nur Alice ist admin
    });

    section('Placeholders');

    test('rawQuery() named Placeholders', function () use ($db) {
        $row = $db->rawQueryOne(
            "SELECT * FROM tst_users WHERE role = :role AND active = :active",
            ['role' => 'admin', 'active' => 1]
        );
        return is_array($row) && $row['name'] === 'Alice Admin';
    });

    test('rawQuery() named Placeholders mit Doppelpunkt', function () use ($db) {
        $row = $db->rawQueryOne(
            "SELECT * FROM tst_users WHERE role = :role AND active = :active",
            [':role' => 'admin', ':active' => 1]
        );
        return is_array($row) && $row['name'] === 'Alice Admin';
    });

    section('LIKE & SOUNDS LIKE');

    test('whereLike() both', function () use ($db) {
        $rows = $db->whereLike('name', 'ditor')->get('users');
        return is_array($rows) && count($rows) === 2; // Bob Editor, Eve Editor
    });

    test('whereLike() left', function () use ($db) {
        $rows = $db->whereLike('name', 'Editor', 'left')->get('users');
        return is_array($rows) && count($rows) === 2; // Bob Editor, Eve Editor
    });

    test('whereLike() right', function () use ($db) {
        $rows = $db->whereLike('name', 'Alice', 'right')->get('users');
        return is_array($rows) && count($rows) === 1; // Alice Admin
    });

    test('whereLike() none', function () use ($db) {
        $rows = $db->whereLike('name', '%ditor', 'none')->get('users');
        return is_array($rows) && count($rows) === 2; // Bob Editor, Eve Editor
    });

    test('whereNotLike()', function () use ($db) {
        $all  = count($db->get('users'));
        $like = count($db->whereLike('name', 'Editor')->get('users'));
        $rows = $db->whereNotLike('name', 'Editor')->get('users');
        return is_array($rows) && count($rows) === $all - $like;
    });

    test('orWhereLike()', function () use ($db) {
        $rows = $db->whereLike('name', 'Alice', 'right')
            ->orWhereLike('name', 'Eve', 'right')
            ->get('users');
        return is_array($rows) && count($rows) === 2;
    });

    test('whereSoundsLike()', function () use ($db) {
        $rows = $db->whereSoundsLike('name', 'Alice Admin')->get('users');
        return is_array($rows) && count($rows) >= 1;
    });

    test('orWhereSoundsLike()', function () use ($db) {
        $rows = $db->whereSoundsLike('name', 'Alice Admin')
            ->orWhereSoundsLike('name', 'Bob Editor')
            ->get('users');
        return is_array($rows) && count($rows) >= 2;
    });

    section('WHERE GROUPS');

    test('openWhereGroup() / closeWhereGroup() – AND', function () use ($db) {
        $rows = $db->whereInt('active', 1)
            ->openWhereGroup('AND')
            ->whereString('role', 'admin')
            ->orWhereString('role', 'editor')
            ->closeWhereGroup()
            ->get('users');
        // active=1 AND (role='admin' OR role='editor') → Alice, Bob, Eve
        return is_array($rows) && count($rows) === 3;
    });

    test('openWhereGroup() – beginnend mit Gruppe', function () use ($db) {
        $rows = $db->openWhereGroup('AND')
            ->whereString('role', 'admin')
            ->orWhereString('role', 'editor')
            ->closeWhereGroup()
            ->whereInt('active', 1)
            ->get('users');
        // (role='admin' OR role='editor') AND active=1 → Alice, Bob, Eve
        return is_array($rows) && count($rows) === 3;
    });

    test('whereGroup() – Callback', function () use ($db) {
        $rows = $db->whereInt('active', 1)
            ->whereGroup(function($q) {
                $q->whereString('role', 'admin')
                    ->orWhereString('role', 'editor');
            }, 'AND')
            ->get('users');
        return is_array($rows) && count($rows) === 3;
    });

    test('whereGroup() – nested', function () use ($db) {
        $rows = $db->whereInt('active', 1)
            ->whereGroup(function($q) {
                $q->whereString('role', 'editor')
                    ->whereGroup(function($q) {
                        $q->whereString('name', 'Bob Editor')
                            ->orWhereString('name', 'Eve Editor');
                    }, 'AND');
            }, 'AND')
            ->get('users');
        // active=1 AND (role='editor' AND (name='Bob Editor' OR name='Eve Editor'))
        return is_array($rows) && count($rows) === 2;
    });

    test('whereGroup() – OR Gruppe', function () use ($db) {
        $withoutGroup = count($db->whereString('role', 'viewer')->get('users'));

        $withGroup = count(
            $db->whereString('role', 'viewer')
                ->whereGroup(function($q) {
                    $q->whereString('name', 'Alice Admin')
                        ->orWhereString('name', 'Bob Editor');
                }, 'OR')
                ->get('users')
        );

        return $withGroup > $withoutGroup;
    });
}

// ============================================================
// == HTML AUSGABE ==
// ============================================================

$totalTime = round((microtime(true) - $scriptStart) * 1000, 2);
$total     = $passed + $failed;
$percent   = $total > 0 ? round($passed / $total * 100) : 0;

?>
<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PDOdb Testresults</title>
    <style>
		* { box-sizing: border-box; margin: 0; padding: 0; }

		body {
			font-family: 'Segoe UI', system-ui, sans-serif;
			background: #0f1117;
			color: #e2e8f0;
			padding: 2rem;
			font-size: 14px;
		}

		h1 {
			font-size: 1.6rem;
			font-weight: 700;
			color: #f8fafc;
			margin-bottom: 0.25rem;
		}

		.meta {
			color: #64748b;
			font-size: 0.85rem;
			margin-bottom: 2rem;
		}

		.meta code {
			background: #1e2330;
			padding: 0.1rem 0.4rem;
			border-radius: 4px;
			color: #94a3b8;
		}

		/* Summary Cards */
		.summary {
			display: flex;
			gap: 1rem;
			margin-bottom: 1.5rem;
			flex-wrap: wrap;
		}

		.card {
			background: #1e2330;
			border: 1px solid #2d3548;
			border-radius: 10px;
			padding: 1rem 1.5rem;
			min-width: 120px;
			text-align: center;
		}

		.card .val {
			font-size: 2rem;
			font-weight: 800;
			line-height: 1;
		}

		.card .lbl {
			font-size: 0.72rem;
			color: #64748b;
			margin-top: 0.3rem;
			text-transform: uppercase;
			letter-spacing: 0.06em;
		}

		.card.green .val { color: #4ade80; }
		.card.red   .val { color: #f87171; }
		.card.blue  .val { color: #60a5fa; }
		.card.gray  .val { color: #94a3b8; }

		/* Progress Bar */
		.progress-wrap {
			background: #1e2330;
			border: 1px solid #2d3548;
			border-radius: 10px;
			padding: 1rem 1.5rem;
			margin-bottom: 2rem;
		}

		.progress-label {
			display: flex;
			justify-content: space-between;
			margin-bottom: 0.5rem;
			font-size: 0.8rem;
			color: #94a3b8;
		}

		.progress-bar {
			height: 10px;
			background: #2d3548;
			border-radius: 99px;
			overflow: hidden;
		}

		.progress-fill {
			height: 100%;
			border-radius: 99px;
			background: linear-gradient(90deg, #4ade80, #22c55e);
		}

		.progress-fill.warn { background: linear-gradient(90deg, #facc15, #f59e0b); }
		.progress-fill.bad  { background: linear-gradient(90deg, #f87171, #ef4444); }

		/* Sections */
		.section {
			margin-bottom: 1rem;
			background: #1e2330;
			border: 1px solid #2d3548;
			border-radius: 10px;
			overflow: hidden;
		}

		.section-header {
			display: flex;
			justify-content: space-between;
			align-items: center;
			padding: 0.7rem 1rem;
			background: #252b3b;
			border-bottom: 1px solid #2d3548;
			cursor: pointer;
			user-select: none;
		}

		.section-header:hover { background: #2d3548; }

		.section-title {
			font-weight: 600;
			font-size: 0.82rem;
			letter-spacing: 0.08em;
			text-transform: uppercase;
			color: #94a3b8;
			display: flex;
			align-items: center;
			gap: 0.5rem;
		}

		.arrow {
			font-size: 0.7rem;
			transition: transform 0.2s;
			display: inline-block;
			color: #64748b;
		}

		.section-badges {
			display: flex;
			gap: 0.4rem;
			align-items: center;
		}

		.badge {
			padding: 0.15rem 0.55rem;
			border-radius: 99px;
			font-size: 0.72rem;
			font-weight: 600;
		}

		.badge.pass { background: #14532d; color: #4ade80; }
		.badge.fail { background: #450a0a; color: #f87171; }
		.badge.time { background: #1e3a5f; color: #60a5fa; }

		/* Test Table */
		table { width: 100%; border-collapse: collapse; }

		tr:not(:last-child) td { border-bottom: 1px solid #1a2030; }

		td { padding: 0.5rem 1rem; vertical-align: middle; }

		.td-status { width: 24px; text-align: center; font-size: 0.9rem; }
		.td-name   { color: #cbd5e1; }
		.td-time   { width: 75px; text-align: right; font-family: monospace; font-size: 0.78rem; white-space: nowrap; }

		.td-info {
			color: #fca5a5;
			font-size: 0.78rem;
			font-family: monospace;
			word-break: break-all;
			margin-top: 0.2rem;
			padding: 0.3rem 0.5rem;
			background: #2a0a0a;
			border-radius: 4px;
			border-left: 3px solid #ef4444;
		}

		tr.fail, tr.error { background: #1a0808; }
		tr.pass:hover     { background: #1a2535; }
		tr.fail:hover,
		tr.error:hover    { background: #200d0d; }

		.time-fast   { color: #4ade80; }
		.time-medium { color: #facc15; }
		.time-slow   { color: #f87171; }

		/* Setup Error */
		.setup-error {
			background: #450a0a;
			border: 1px solid #7f1d1d;
			border-radius: 10px;
			padding: 1.5rem;
			color: #fca5a5;
			margin-bottom: 2rem;
		}

		.setup-error strong { display: block; margin-bottom: 0.5rem; font-size: 1rem; color: #f87171; }

		/* Collapsed state */
		.section-body.hidden { display: none; }
		.arrow.rotated { transform: rotate(-90deg); }
    </style>
</head>
<body>

<h1>PDOdb – Testresults</h1>
<p class="meta">
    <?= date('d.m.Y H:i:s') ?> &nbsp;·&nbsp;
    PHP <?= PHP_VERSION ?> &nbsp;·&nbsp;
    DB: <code><?= DB_NAME ?></code> &nbsp;·&nbsp;
    Prefix: <code><?= DB_PREFIX ?></code> &nbsp;·&nbsp;
    Gesamtzeit: <strong style="color:#60a5fa"><?= $totalTime ?> ms</strong>
</p>

<?php if ($setupError): ?>
    <div class="setup-error">
        <strong>⛔ Datenbankfehler beim Setup</strong>
        <?= htmlspecialchars($setupError) ?>
    </div>
<?php else: ?>

    <div class="summary">
        <div class="card <?= $percent === 100 ? 'green' : ($percent >= 80 ? 'blue' : 'red') ?>">
            <div class="val"><?= $percent ?>%</div>
            <div class="lbl">Erfolgsrate</div>
        </div>
        <div class="card green">
            <div class="val"><?= $passed ?></div>
            <div class="lbl">Passed</div>
        </div>
        <div class="card <?= $failed > 0 ? 'red' : 'gray' ?>">
            <div class="val"><?= $failed ?></div>
            <div class="lbl">Failed</div>
        </div>
        <div class="card blue">
            <div class="val"><?= $total ?></div>
            <div class="lbl">Gesamt</div>
        </div>
        <div class="card gray">
            <div class="val"><?= $totalTime ?></div>
            <div class="lbl">ms gesamt</div>
        </div>
    </div>

    <div class="progress-wrap">
        <div class="progress-label">
            <span>Fortschritt</span>
            <span><?= $passed ?> / <?= $total ?> Tests bestanden</span>
        </div>
        <div class="progress-bar">
            <div class="progress-fill <?= $percent < 60 ? 'bad' : ($percent < 90 ? 'warn' : '') ?>"
                 style="width:<?= $percent ?>%"></div>
        </div>
    </div>

    <?php
    $sections = [];
    $current  = null;

    foreach ($results as $r) {
        if ($r['type'] === 'section') {
            if ($current !== null) $sections[] = $current;
            $current = ['name' => $r['name'], 'tests' => []];
        } else {
            if ($current !== null) $current['tests'][] = $r;
        }
    }
    if ($current !== null) $sections[] = $current;

    foreach ($sections as $sec):
        $secPassed = count(array_filter($sec['tests'], fn($t) => $t['status'] === 'PASS'));
        $secFailed = count($sec['tests']) - $secPassed;
        $secTime   = round(array_sum(array_column($sec['tests'], 'ms')), 2);
        ?>
        <div class="section">
            <div class="section-header" onclick="toggle(this)">
    <span class="section-title">
      <span class="arrow">▼</span>
      <?= htmlspecialchars($sec['name']) ?>
    </span>
                <span class="section-badges">
      <span class="badge pass"><?= $secPassed ?> ✓</span>
      <?php if ($secFailed > 0): ?>
          <span class="badge fail"><?= $secFailed ?> ✗</span>
      <?php endif; ?>
      <span class="badge time"><?= $secTime ?> ms</span>
    </span>
            </div>
            <div class="section-body">
                <table>
                    <?php foreach ($sec['tests'] as $t):
                        $cls  = strtolower($t['status']);
                        $icon = match($t['status']) { 'PASS' => '✅', 'FAIL' => '❌', 'ERROR' => '💥' };
                        $timeCls = $t['ms'] < 5 ? 'time-fast' : ($t['ms'] < 30 ? 'time-medium' : 'time-slow');
                        ?>
                        <tr class="<?= $cls ?>">
                            <td class="td-status"><?= $icon ?></td>
                            <td class="td-name">
                                <?= htmlspecialchars($t['name']) ?>
                                <?php if ($t['info']): ?>
                                    <div class="td-info">→ <?= htmlspecialchars($t['info']) ?></div>
                                <?php endif; ?>
                            </td>
                            <td class="td-time <?= $timeCls ?>"><?= $t['ms'] ?> ms</td>
                        </tr>
                    <?php endforeach; ?>
                </table>
            </div>
        </div>
    <?php endforeach; ?>

<?php endif; ?>

<script>
    function toggle(header) {
        const body  = header.nextElementSibling;
        const arrow = header.querySelector('.arrow');
        body.classList.toggle('hidden');
        arrow.classList.toggle('rotated');
    }
</script>
</body>
</html>