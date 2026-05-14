# PDOdb – A Secure MySQL Query Builder for PHP 8+

PDOdb is a modern database wrapper built on top of PDO. It gives you a clean, expressive API for building queries without writing raw SQL by hand – while staying safe, strict and fast by default.

Inspired by [ThingEngineer/MySQLiDb](https://github.com/ThingEngineer/MysqliDb), but rewritten from scratch for PDO with full type-safe WHERE methods, strict injection protection and a fluent or step-by-step API – your choice.

📘 **Documentation:** https://DocsPDOdb.decmuc.dev

---

## Why PDOdb?

Because writing `$pdo->prepare("SELECT...")` for the hundredth time is boring – and forgetting to bind a parameter is how things go wrong.

PDOdb handles the boring parts. You write what you mean:

```php
$user = $db->whereInt('id', 42)->getOne('users');
// → SELECT * FROM users WHERE id = 42 LIMIT 1
```

No manual prepare, no manual bind, no white screen on error. Exceptions all the way.

---

## Installation

```bash
composer require decmuc/pdodb
```

Or [download manually](https://github.com/decMuc/PDOdb/releases/latest) – PDOdb is a single file.

---

## Quick Start

```php
require 'vendor/autoload.php';
use decMuc\PDOdb\PDOdb;

$db = new PDOdb([
    'host'     => 'localhost',
    'username' => 'root',
    'password' => 'secret',
    'db'       => 'my_database',
    'charset'  => 'utf8mb4',
]);
```

Both styles work – use whichever fits your code:

```php
// Step by step – easy to read and extend
$db->whereInt('active', 1);
$db->whereString('role', 'admin');
$db->orderBy('created_at', 'DESC');
$users = $db->get('users');

// Fluent – compact for experienced devs
$users = $db->whereInt('active', 1)
            ->whereString('role', 'admin')
            ->orderBy('created_at', 'DESC')
            ->get('users');

// → SELECT * FROM users WHERE active = 1 AND role = 'admin' ORDER BY created_at DESC
```

---

## Features

- ✅ **Type-safe WHERE methods** – `whereInt()`, `whereFloat()`, `whereString()`, `whereBool()`, `whereDate()`, `whereLike()`, `whereSoundsLike()` and more
- ✅ **Prepared statements** – all values are bound automatically, never interpolated
- ✅ **Heuristic injection check** – blocks obvious attack patterns like `1 OR 1=1` before they even reach the DB
- ✅ **Fluent or step-by-step API** – both work, both produce the same SQL
- ✅ **WHERE grouping** – `whereGroup()` for parenthesized conditions
- ✅ **Joins & subqueries** – with prefix support and aliasing
- ✅ **Transactions** – `startTransaction()`, `commit()`, `rollback()`
- ✅ **Bulk insert & pagination** – `insertBulk()`, `paginate()`
- ✅ **Table prefix** – set once, applied everywhere automatically
- ✅ **Multiple instances** – connect to several databases side by side
- ✅ **Named placeholders** in `rawQuery()` – both `:name` and `name` style
- ✅ **Debug & trace** – `setTrace(true)`, `getLastDebugQuery()`, debug levels 0–3

---

## A Few More Examples

```php
// Boolean flags – the clean way
$db->whereBool('is_active', true)->get('users');
// → SELECT * FROM users WHERE is_active = 1

// Date range
$db->whereDateBetween('created_at', '2025-01-01', '2025-12-31')->get('orders');
// → SELECT * FROM orders WHERE created_at BETWEEN '2025-01-01' AND '2025-12-31'

// LIKE with wildcard modes
$db->whereLike('name', 'john', 'both')->get('users');
// → SELECT * FROM users WHERE name LIKE '%john%'

// WHERE groups with parentheses
$db->whereInt('active', 1)
   ->whereGroup(function($q) {
       $q->whereString('role', 'admin')
         ->orWhereString('role', 'editor');
   })
   ->get('users');
// → SELECT * FROM users WHERE active = 1 AND (role = 'admin' OR role = 'editor')

// Raw query with named placeholders
$db->rawQuery("SELECT * FROM users WHERE id = :id AND role = :role", [
    'id'   => 42,
    'role' => 'admin',
]);

// Insert & get ID
$id = $db->insert('users', [
    'name'  => 'Alice',
    'email' => 'alice@example.com',
    'active' => 1,
]);

// Transactions
$db->startTransaction();
try {
    $db->insert('orders', ['user_id' => 1, 'total' => 49.99]);
    $db->update('users', ['last_order' => date('Y-m-d')], $db->whereInt('id', 1));
    $db->commit();
} catch (\Throwable $e) {
    $db->rollback();
}
```

---

## Error Handling

PDOdb throws real exceptions – no more white screens, no more silent failures.

```php
try {
    $db->whereInt('id', 'not-a-number');
    $user = $db->getOne('users');
} catch (\InvalidArgumentException $e) {
    echo $e->getMessage();
    // → Invalid integer value for column 'id'.
}
```

---

## Roadmap

**v1.4.0** *(current)*
- `whereLike()` / `whereNotLike()` with wildcard modes (`both`, `left`, `right`, `none`)
- `whereSoundsLike()` using MySQL `SOUNDS LIKE`
- `whereGroup()` / `openWhereGroup()` / `closeWhereGroup()` for parenthesized conditions
- Named placeholders in `rawQuery()`
- Several bug fixes (JOIN prefix, subquery bind params, `copy()` with PDO)

**v2.0.0** *(planned)*
- Multi-driver support: **PostgreSQL**, **SQLite** (and later MSSQL)
- Same API, different dialects internally – your code stays the same
- Compatibility table per method in the docs (✅ MySQL ✅ PostgreSQL ⚠️ SQLite)

---

## Documentation

Full documentation with examples for every method:
👉 **https://DocsPDOdb.decmuc.dev**

---

## License

MIT – see [LICENSE](LICENSE) for details.

---

*Built with ❤️ by [L. Fischer (decMuc)](https://github.com/decMuc)*
