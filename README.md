# PDOdb – Modern PDO MySQL Database Class for PHP 8+

[![PHP Version](https://img.shields.io/badge/php-%3E=8.0-blue)](https://www.php.net/manual/en/migration80.php)
[![License](https://poser.pugx.org/decmuc/pdodb/license)](https://packagist.org/packages/decmuc/pdodb)
[![Composer Ready](https://img.shields.io/badge/Composer-ready-blueviolet)](https://packagist.org/packages/decmuc/pdodb)
[![Packagist Version](https://img.shields.io/packagist/v/decmuc/pdodb.svg)](https://packagist.org/packages/decmuc/pdodb)

**PDOdb** is a modern, secure, and fully compatible rewrite of the popular and awesome  [PHP-MySQLi-Database-Class by ThingEngineer](https://github.com/ThingEngineer/PHP-MySQLi-Database-Class) – designed as a direct replacement but built entirely on PHP’s native PDO extension.

The goal of this project is to provide a **close to 1:1** logic-compatible alternative to the original class, allowing existing projects to migrate seamlessly to a safer, cleaner, and future-proof implementation – without rewriting business logic or adjusting method calls in most cases, except for a few legacy features that were intentionally dropped due to security, portability, or PDO-incompatibility.

## Why switch from the original?

While the original MysqliDb class was a great utility for many years, it is no longer actively maintained and relies on the outdated `mysqli_*` extension. Its architecture can pose security and maintainability risks in modern applications.

**PDOdb** addresses these concerns and offers numerous improvements:

## 🔑 Key Features

- 🔄 ThingEngineer-inspired API
  Familiar method names and structure, modernized for PDO and security. Some legacy features were dropped intentionally.

- 🔐 **Built on native PDO** 
  Replaces `mysqli_*` with secure and flexible PDO, using real prepared statements.

- 🧱 **Modern PHP: Clean, typed, and robust**
  Written from scratch in PHP 8+, with strict types, union types, and modern error handling.

- 🧩 **Namespaced and PSR-3 compatible**
  Ships with proper namespacing (`decMuc\PDOdb`) and optional PSR-3 logger support.

- ♻️ **Extensible and modular**
  Easily add custom functionality or override behavior thanks to clean class design.

- 🛡️ **Secure by design**
  Prepared statements, typed parameters, and internal validation reduce common attack vectors like SQL injection.

- 🔍 **Support for complex queries**
  JOINs, subqueries, pagination, transactions, and custom SQL – all handled seamlessly.

- 🧪 **Trace and debug utilities**
  Built-in query tracing, debug output, and caller info to help during development.

- 🔁 **Multi-connection support**
  Switch between multiple database connections on the fly (`$db->connection('read')`, etc.).

- 🔌 **Simultaneous connections to different databases**
  Easily connect to and switch between multiple databases (e.g. master/slave, customer-specific) via `$db->addConnection()` and `$db->connection()`.

- 🚀 **Bulk operations**
  Supports `insertMulti()` and `insertBulk()` for efficient mass inserts.

- 📦 **100% self-contained**
  No external dependencies – just drop it into your project or install via Composer.


---

## 📚 Table of Contents

- [Installation](#installation)
- [⚠️ Warnings](#warnings)
- [Objects Mapping (coming soon)](#objects-mapping-coming-soon)
- [Initialization](#initialization)
- [Insert Query](#insert-query)
- [InsertMulti Query](#insertmulti-query)
- [InsertBulk Query](#insertbulk-query)
- [Update Query](#update-query)
- [Select Query](#select-query)
- [Delete Query](#delete-query)
- [Insert Data](#insert-data)
- [Insert XML](#insert-xml)
- [Pagination](#pagination)
- [Raw Queries](#raw-queries)
- [Query Keywords](#query-keywords)
- [Where Conditions](#where-conditions)
- [Order Conditions](#order-conditions)
- [Group Conditions](#group-conditions)
- [Properties Sharing](#properties-sharing)
- [Joining Tables](#joining-tables)
- [Subqueries](#subqueries)
- [SQL Conditions (EXISTS, NOT EXISTS, HAS, ...)](#sql-conditions-exists-not-exists-has-)
- [Lock / Unlock](#lock--unlock)
- [Transactions](#transactions)
- [Debug](#debug)
  - [Debugging](#debugging)
  - [Trace and Profiling](#trace-and-profiling)

<hr>

### Installation

You can install PDOdb in two ways:

🔹 1. Via Composer (recommended)
Use the latest development version to always stay up to date:

```
composer require decMuc/pdodb:dev-master
```

Alternatively, you can use a tagged release (if available):
```
composer require decMuc/pdodb
```
ℹ️ This will automatically register the autoloader and allow you to use decMuc\PDOdb\PDOdb in your project.


🔹 2. Manual installation
If you don't use Composer, simply download the class and include it manually:

```php
require_once '/path/to/PDOdb.php';
use decMuc\PDOdb\PDOdb;
```
⚠️ Be sure to include all required class files if you separate them into multiple files/folders.


**⚠️ WARNING**

PDOdb is designed to mimic the behavior of ThingEngineer's MysqliDb for compatibility purposes – but just like the original class, it is not bulletproof if misused.

Here are a few important things to keep in mind:

🔐 Always validate user input – just because the class uses prepared statements internally does not mean it's safe to pass unchecked values into dynamic query components (like table names or raw SQL).

❗ **Never pass untrusted input directly into methods like:**

```php
$db->get($_POST['table'])

$db->update($_POST['table'], $data)

$db->query("SELECT * FROM " . $_GET['table'])
```

🛡️ PDOdb protects values via bound parameters, but does not validate structure, table names, or SQL fragments unless you explicitly implement those checks (e.g. via _validateTableName() or strict method usage).

❗ **We strongly recommend that you whitelist table names and sanitize all dynamic input, even when using this class in trusted environments.**

✅ PDOdb provides internal protection mechanisms like:

- Table name validation (_validateTableName())
- Strict parameter types
- Optional debug/tracing
- Prefix safety

Remember: even the safest database layer can't protect you from insecure logic. Security starts with architecture.


## Objects Mapping (coming soon)

## Initialization
You can initialize the database handler in multiple ways – either using classic parameter passing or via an associative configuration array.


```php
// with manual installation
require_once '/path/to/PDOdb.php';
use decMuc\PDOdb\PDOdb;

// or with composer autoload
require_once '/vendor/autoload.php';
```


**Example 1:** Simple Initialization with Parameters

```php
$db = new PDOdb('localhost', 'root', 'secret', 'my_database');
```

**Example 2:** Initialization with Config Array

```php
// create a default instance
$db = new PDOdb([
    'host'     => 'localhost',
    'username' => 'root',
    'password' => 'secret',
    'db'       => 'my_database',
    'port'     => 3306,
    'charset'  => 'utf8mb4'
]);

// creates a named instance
$db2 = new PDOdb([
    'host'     => 'localhost',
    'username' => 'root',
    'password' => 'secret',
    'db'       => 'my_database',
    'port'     => 3306,
    'charset'  => 'utf8mb4',
    'instance' => 'customer'
]);

// Retrieve active or named instance:
$db    = PDOdb::getInstance();              // returns last active default
$db2   = PDOdb::getInstance('customer');        // explicitly retrieve 'customer'
```

**Example 3:** Using an existing PDO instance

```php
$pdo = new PDO(
    'mysql:host=localhost;dbname=my_database;charset=utf8mb4',
    'root',
    'secret',
    [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
);

$db = new PDOdb($pdo);
```

**Example 4:** Reusing a globally shared PDOdb instance
```php
function init() {
    // Create and register global instance
    $db = new \decMuc\PDOdb\PDOdb([
          'host'     => 'localhost',
          'username' => 'root',
          'password' => 'secret',
          'db'       => 'my_database',
          'port'     => 3306,
          'charset'  => 'utf8mb4'
      ]);
}

function myFunction() {
    // Retrieve globally shared instance
    $db = \decMuc\PDOdb\PDOdb::getInstance();
    ...
}
```

## Multiple database connection
If you need to connect to multiple databases use following method:

```php
$db2 = new PDOdb([
     'host'     => 'localhost',
     'username' => 'root',
     'password' => 'secret',
     'db'       => 'my_database',
     'port'     => 3306,
     'charset'  => 'utf8mb4'
 ]);

// or
$db->addConnection('slave', [
    'host'     => 'localhost',
    'username' => 'root',
    'password' => 'secret',
    'db'       => 'my_database',
    'port'     => 3306,
    'charset'  => 'utf8mb4'
]);
```

To select database use connection() method
```php
$users = $db->connection('slave')->get('users');
```

**Example:** Initialisation for Multi-Tenant Environments
If your application handles multiple customers (tenants) with separate databases, you can encapsulate the logic in a custom factory or helper class:

```php
namespace App\Data\Library;

use \decMuc\PDOdb\PDOdb;

class CustDB
{
    /**
     * Creates a new PDOdb instance with arbitrary credentials
     */
    public static function createInstance(
        string $host,
        string $username,
        string $password,
        string $dbName,
        int $port = 3306,
        string $charset = 'utf8mb4',
        string $instance = 'customer'
    ): PDOdb {
        return new PDOdb([
            'host'     => $host,
            'username' => $username,
            'password' => $password,
            'db'       => $dbName,
            'port'     => $port,
            'charset'  => $charset,
            'instance' => $instance // important!! without you got the last default
        ]);
    }
}
```

## Insert Query
Inserts a single row into the database.
```php
$data = [
    "login" => "admin",
    "firstName" => "John",
    "lastName" => 'Doe'
];
$id = $db->insert('users', $data);
if($id)
    echo 'user was created. Id=' . $id;
```
🔁 **Return value**
Returns the inserted ID on success (if primary key is auto-increment).

Returns true for success without auto-increment.

Returns false on failure.

🧩 **Insert with SQL Functions**
You can pass SQL functions directly by using `$db->func()`, `$db->now()` or `$db->currentDate()`:
```php
$data = [
    'login'      => 'admin',
    'active'     => true,
    'firstName'  => 'John',
    'lastName'   => 'Doe',
    'password'   => $db->func('SHA1(?)', ['secretpassword+salt']),
    'createdAt'  => $db->now(''),         // 👉 NOW()
    'updatedAt'  => $db->currentDate(1),  // 👉 1 = Y-m-d H:i:s (0 = Y-m-d, 2 = UNIX timestamp)
    'expires'    => $db->now('+1Y')       // 👉 NOW() + INTERVAL 1 YEAR
];
$id = $db->insert('users', $data);
if ($id) {
    echo 'User was created. ID = ' . $id;
} else {
    echo 'Insert failed: ' . $db->getLastError();
}
```

**Insert with on duplicate key update**
```php
$data = [
    'login'     => 'admin',
    'firstName' => 'John',
    'lastName'  => 'Doe',
    'createdAt' => $db->now(''),       // NOW()
    'updatedAt' => $db->now('')        // NOW()
];

$updateColumns = ['updatedAt'];       // Only update this field on duplicate
$lastInsertId  = 'id';                // Returns the ID on insert, true/false on update

$db->onDuplicate($updateColumns, $lastInsertId);
$id = $db->insert('users', $data);

if ($id) {
    echo "Insert or update successful. ID: $id";
} else {
    echo "Insert failed: " . $db->getLastError();
}
```

## InsertMulti Query
```php
$data = [
    [ 'login' => 'admin', 'firstName' => 'John', 'lastName' => 'Doe' ],
    [ 'login' => 'other', 'firstName' => 'Another', 'lastName' => 'User', 'password' => 'very_cool_hash' ]
];

$ids = $db->insertMulti('users', $data);
if (!$ids) {
    echo 'Insert failed: ' . $db->getLastError();
} else {
    echo 'New users inserted with following IDs: ' . implode(', ', $ids);
}
```
If all datasets share the same structure, the call can be simplified:
```php
$data = [
    ['admin', 'John', 'Doe'],
    ['other', 'Another', 'User']
];
$keys = ['login', 'firstName', 'lastName'];

$ids = $db->insertMulti('users', $data, $keys);
```

⚠️ Note: insertMulti() inserts each row individually using prepared statements.
While it's secure and flexible, it is slower for large data sets.
For performance-critical bulk inserts, consider using insertBulk(), which generates a single optimized SQL statement for all rows.

## InsertBulk Query
The insertBulk() method allows you to insert many rows in a single SQL query, making it significantly faster than repeated single inserts or even insertMulti() in larger datasets.

This method is ideal for high-performance imports, batch processing, and when inserting thousands of records at once.

⚠️ Warning
Unlike insertMulti(), insertBulk() does not return the inserted IDs, but instead gives you the total number of successfully inserted rows (row count). This trade-off ensures better speed and less overhead.

**Example:** Inserting 1000 products efficiently
```php
$rows = [];
for ($i = 1; $i <= 1000; $i++) {
    $rows[] = [
        'name'        => 'Product ' . $i,
        'category_id' => rand(1, 5),
        'supplier_id' => rand(1, 5),
        'price'       => rand(10, 999) + 0.99
    ];
}

$count = $db->insertBulk('test_products_bulk', $rows);

if ($count) {
    echo "Inserted $count records successfully.";
} else {
    echo "Insert failed: " . $db->getLastError();
}
```

**📊 Benchmark: insertBulk vs insertMulti**

We’ve included a live performance comparison script to demonstrate the time difference between `insertMulti()` and `insertBulk()`:

👉 See examples/multi-bulk-benchmark.php

This benchmark inserts 1000 records using both methods and shows the execution time and total rows inserted for each approach.

## Replace Query

The `replace()` method works similarly to `insert()`, but uses the SQL command REPLACE INTO instead of INSERT.

This means:

- If a row with the same primary key or unique key already exists, it will be deleted and replaced.

- If no matching key exists, it behaves like a normal INSERT.

This method is useful for upserts, but note that REPLACE deletes the existing row first, which may affect foreign keys or triggers.

**Example: Replace a user**
```php
$data = [
    'id'        => 1,                  // Must match an existing unique key
    'login'     => 'admin',
    'firstName' => 'John',
    'lastName'  => 'Doe',
    'updatedAt' => $db->currentDate(1)
];

$id = $db->replace('users', $data);

if ($id) {
    echo "User was replaced. ID: " . $id;
} else {
    echo "Replace failed: " . $db->getLastError();
}
```
⚠️ Be aware
- REPLACE is not the same as ON DUPLICATE KEY UPDATE. It deletes the row and inserts a new one.

- Auto-increment values may change.

- Associated rows in foreign tables may break unless ON DELETE CASCADE is defined.

## Update Query
```php
$data = [
    'firstName'  => 'Bobby',
    'lastName'   => 'Tables',
    'editCount'  => $db->inc(2),     // editCount = editCount + 2
    'active'     => $db->not()       // active = !active
];

$db->where('id', 1);
if ($db->update('users', $data)) {
    echo $db->count . ' records were updated';
} else {
    echo 'update failed: ' . $db->getLastError();
}
```

`update()` also support limit parameter:

```php
$db->update('users', $data, 10);
// Generates: UPDATE users SET ... LIMIT 10
```

## Select Query
After executing a get()-like method, the number of returned rows will be available in $db->count.
```php
$users = $db->get('users');
// returns all users

$users = $db->get('users', 10);
// returns the first 10 users
```

Select specific columns

```php
$columns = ['id', 'login', 'email'];
$users = $db->get('users', null, $columns);

if ($db->count > 0) {
    foreach ($users as $user) {
        echo "{$user['id']}: {$user['login']} ({$user['email']})\n";
    }
}
```

Select a single row
```php
$db->where('id', 1);
$user = $db->getOne('users');

if ($user) {
    echo "Welcome back, {$user['login']}";
}
```

Select a single row with custom expressions
```php
$stats = $db->getOne('users', 'SUM(id) AS total, COUNT(*) AS cnt');

echo "Total ID sum: {$stats['total']}, total users: {$stats['cnt']}";
```

Select a single value
```php
$total = $db->getValue('users', 'COUNT(*)');
echo "{$total} users found";
```

Select a single column from multiple rows
```php
$logins = $db->getValue('users', 'login', null);
// SELECT login FROM users

$logins = $db->getValue('users', 'login', 5);
// SELECT login FROM users LIMIT 5

foreach ($logins as $login) {
    echo "Login: $login\n";
}
```


## Insert Data
## ❌ Legacy: loadData() and loadXml() \[Removed]

These two methods were part of the original ThingEngineer MySQLi wrapper and allowed direct file imports via:

* `LOAD DATA INFILE` (CSV import)
* `LOAD XML INFILE` (XML import)

```php
$db->loadData("users", "/path/to/file.csv");
$db->loadXml("users", "/path/to/file.xml");
```
**⚠️ Why were they removed?**

* They rely on non-portable SQL features that bypass PDO

* They require special MySQL privileges (FILE, local_infile, etc.)

* They are incompatible with prepared statements and the security model of PDO

* They caused inconsistent behavior on shared hosting or managed environments

**✅ Alternative**

If you need to import CSV or XML data:

Use standard PHP functions (fgetcsv(), XMLReader, etc.)

Insert using `$db->insert()` or `$db->insertMulti()` with prepared data


```php
if (($fp = fopen('users.csv', 'r')) !== false) {
    while (($row = fgetcsv($fp, 0, ';')) !== false) {
        $db->insert('users', [
            'name' => $row[0],
            'email' => $row[1],
            // ...
        ]);
    }
    fclose($fp);
}
```

## Pagination
Use `paginate()` instead of `get()` to automatically handle page-based results.
```php
$page = 1;

// Set number of results per page (default is 20)
$db->pageLimit = 2;

// Fetch paginated results using arrayBuilder()
$products = $db->arrayBuilder()->paginate("products", $page);

echo "Showing page $page out of " . $db->totalPages;
```

Each call to `paginate()` updates:

`$db->totalPages` — number of available pages

`$db->totalCount` — total number of matching records

`$db->count` — number of results in current page

ℹ️ You can combine `where()`, `join()`, and all other query builders just like with `get()`.

## Raw Queries
You can execute any SQL query directly using `rawQuery()` with optional bindings:
```php
$users = $db->rawQuery('SELECT * FROM users WHERE id >= ?', [10]);
foreach ($users as $user) {
    print_r($user);
}
```
🛠️ Helper methods for raw queries
Get a single row (associative array):
```php
$user = $db->rawQueryOne('SELECT * FROM users WHERE id = ?', [10]);
echo $user['login'];
```
Get a single value (column):
```php
$password = $db->rawQueryValue('SELECT password FROM users WHERE id = ? LIMIT 1', [10]);
echo "Password is {$password}";
```

Get a single column from multiple rows:
```php
$logins = $db->rawQueryValue('SELECT login FROM users LIMIT 10');
foreach ($logins as $login) {
    echo $login;
}
```
✅ IMPORTANT!!! Correct usage
Always use placeholders and bound values to avoid SQL injection:

❌ Wrong usage — do not do this:
```php
$id = $_GET['id'];
// ❌ Dangerous: exposes your database to SQL injection!
$users = $db->rawQuery("SELECT * FROM users WHERE id = $id");

// ✅ Instead, use:
$users = $db->rawQuery("SELECT * FROM users WHERE id = ?", [$id]);
```

**Advanced Example**
Even complex UNION queries are supported:

```php
$params = [10, 1, 10, 11, 2, 10];
$q = "(
    SELECT a FROM t1 WHERE a = ? AND B = ? ORDER BY a LIMIT ?
) UNION (
    SELECT a FROM t2 WHERE a = ? AND B = ? ORDER BY a LIMIT ?
)";
$results = $db->rawQuery($q, $params);
print_r($results);
```

## Query Keywords
To add LOW PRIORITY | DELAYED | HIGH PRIORITY | IGNORE and the rest of the mysql keywords to INSERT (), REPLACE (), GET (), UPDATE (), DELETE() method or FOR UPDATE | LOCK IN SHARE MODE into SELECT ():
```php
$db->setQueryOption('LOW_PRIORITY')->insert($table, $param);
// Produces: INSERT LOW_PRIORITY INTO table ...
```
```php
$db->setQueryOption('FOR UPDATE')->get('users');
// Produces: SELECT * FROM users FOR UPDATE;
```

Multiple options:
```php
$db->setQueryOption(['LOW_PRIORITY', 'IGNORE'])->insert($table, $param);
// Produces: INSERT LOW_PRIORITY IGNORE INTO table ...
```

In SELECT queries:
```php
$db->setQueryOption('SQL_NO_CACHE');
$users = $db->get("users");
// Produces: SELECT SQL_NO_CACHE * FROM users;
```
ℹ️ You can chain multiple calls to setQueryOption() if needed — each call replaces the previous options unless you manually merge them.

**Method Chaining**
You can use method chaining to fluently build queries:

```php
$results = $db
    ->where('id', 1)
    ->where('login', 'admin')
    ->get('users');
// Produces: SELECT * FROM users WHERE id = 1 AND login = 'admin'
```
This helps keep your code clean and readable when building dynamic conditions.

## Where Conditions

For a 1:1 migration of the old `where()` conditions from the ThingEngineer MySQLi class, you can continue to use the familiar methods.
They have been improved and largely remain compatible but do not provide 100% protection against SQL injection — especially in cases like:
```php
id = 1); DROP TABLE users --
```
Since MySQL often parses integer columns by extracting the leading numeric value (e.g., 1 from 1); DROP...), such injections may silently pass through.

**Recommendation**

We recommend using the new, typed where* methods which offer enhanced security, such as:

- `whereInt()` — safe integer filtering

- `whereString()` — validated string values

- `whereDate()` — validated date input

- `whereIsNull()` / `whereIsNotNull()` — explicit NULL checks

- `whereBool()` — strictly typed boolean values

- `whereIn()` / `whereNotIn()` — safe set membership checks

These methods prevent common attack vectors by enforcing strict type checks and ensure that malicious input cannot be converted into dangerous SQL commands.


**Simple equality check:**

```php
$db->where('id', 123);
$user = $db->get('users');
// SELECT * FROM users WHERE id = 123
```

**Custom comparison operators:**

```php
$db->where('age', 18, '>=');
$db->where('status', ['active', 'pending'], 'IN');
```

**Multiple conditions:**

```php
$db->where('country', 'DE');
$db->where('emailVerified', true);
```

**AND / OR logic (grouped conditions):**

```php
$db->where('type', 'admin')
   ->orWhere('type', 'manager');
// WHERE type = 'admin' OR type = 'manager'
```

**LIKE conditions:**

```php
$name = 'John';
$db->where('name', '%'.$name.'%', 'LIKE');
// WHERE name LIKE '%John%'
```

**BETWEEN conditions:**

```php
$db->where('createdAt', ['2024-01-01', '2024-12-31'], 'BETWEEN');
// WHERE createdAt BETWEEN '2024-01-01' AND '2024-12-31'
```

**Nested conditions using closures:**

```php
$db->where(function($db) {
    $db->where('type', 'admin');
    $db->orWhere('type', 'editor');
});
$db->where('active', true);
```

This will generate:

```sql
WHERE (type = 'admin' OR type = 'editor') AND active = 1
```

**Warning about old `where()` methods**

>⚠️ Warning: The legacy where() methods (compatible with ThingEngineer MySQLi) are not 100% secure. They rely on parameterization but lack strict type enforcement, allowing edge cases like id = 1); DROP TABLE users -- to bypass protections because MySQL interprets the initial numeric part and ignores the rest.

**Secure typed `where*()` methods**

>✅ All the following typed where*() methods implement strict type validation and proper parameter binding, greatly reducing the risk of SQL injection. They should be preferred for all new code and migration efforts:

**NEW `whereInt()` and `orWhereInt()`**
These methods enable safe and type-checked integer filtering in your SQL WHERE clauses.
```php
// Find user with id = 42
$db->whereInt('id', 42);
$user = $db->getOne('users');

// Find users with id = 42 or id = 99
$db->whereInt('id', 42)
   ->orWhereInt('id', 99);
$users = $db->get('users');
```
**What `whereInt()` blocks**
`whereInt()` blocks any non-integer or malformed input to prevent injection and invalid queries.
```php
// Valid usage — allowed
$db->whereInt('id', 123);

// Invalid usage — throws exception or blocks
$db->whereInt('id', '123abc');    // Non-pure integer string
$db->whereInt('id', '1 OR 1=1');  // Injection attempt
$db->whereInt('id', 'DROP TABLE'); // Injection attempt
```
In these cases, `whereInt()` will raise an error or reject the input, preventing unsafe SQL from being executed.

**NEW `whereString()` and `orWhereString()`**
These methods enable safe and type-checked string filtering in your SQL WHERE clauses.

```php
// Find user with username 'john_doe'
$db->whereString('username', 'john_doe');
$user = $db->getOne('users');

// Find users with username 'john_doe' or 'jane_smith'
$db->whereString('username', 'john_doe')
->orWhereString('username', 'jane_smith');
$users = $db->get('users');
```
**What `whereString()` blocks**

`whereString()` blocks any input containing invalid characters or potential injection payloads.

```php
// Valid usage — allowed
$db->whereString('username', 'valid_user');

// Invalid usage — throws exception or blocks
$db->whereString('username', "john'; DROP TABLE users; --");  // Injection attempt
$db->whereString('username', "admin OR 1=1");                 // Injection attempt
```
In these cases, `whereString()` will raise an error or reject the input, preventing unsafe SQL from being executed.

**NEW `whereDate()` and `orWhereDate()`**
These methods enable safe and validated date filtering in your SQL WHERE clauses.

```php
// Find users created on 2025-07-01
$db->whereDate('created_at', '2025-07-01');
$user = $db->getOne('users');

// Find users created on 2025-07-01 or 2025-07-02
$db->whereDate('created_at', '2025-07-01')
->orWhereDate('created_at', '2025-07-02');
$users = $db->get('users');
```
**What `whereDate()` blocks**

`whereDate()` blocks any input that is not a valid date or datetime string.

```php
// Valid usage — allowed
$db->whereDate('created_at', '2025-07-01');

// Invalid usage — throws exception or blocks
$db->whereDate('created_at', '2025-07-01; DROP TABLE users'); // Injection attempt
$db->whereDate('created_at', 'invalid-date');                 // Invalid format
```
In these cases, `whereDate()` will raise an error or reject the input, preventing unsafe SQL from being executed.

**NEW `whereBool()` and `orWhereBool()`**
These methods enable safe and strictly typed boolean filtering in your SQL WHERE clauses.

```php
// Find active users
$db->whereBool('is_active', true);
$users = $db->get('users');

// Find inactive or banned users
$db->whereBool('is_active', false)
->orWhereBool('is_banned', true);
$users = $db->get('users');
```
**What `whereBool()` blocks**

`whereBool()` blocks any input that is not strictly boolean or convertible to 0 or 1.

```php
// Valid usage — allowed
$db->whereBool('is_active', true);
$db->whereBool('is_active', 0);

// Invalid usage — throws exception or blocks
$db->whereBool('is_active', 'yes');   // Invalid boolean
$db->whereBool('is_active', '1 OR 1=1'); // Injection attempt
```
In these cases, `whereBool()` will raise an error or reject the input, preventing unsafe SQL from being executed.

**NEW `whereIn()` and `orWhereIn()`**
These methods enable safe and parameterized filtering by sets in your SQL WHERE clauses.

```php
// Find users with ids in 1, 2, 3
$db->whereIn('id', [1, 2, 3]);
$users = $db->get('users');

// Find users with status in 'active' or 'pending'
$db->whereIn('status', ['active', 'pending'])
->orWhereIn('status', ['banned', 'suspended']);
$users = $db->get('users');
```
**What `whereIn()` blocks**

`whereIn()` blocks empty arrays and ensures all values are safely bound as parameters, preventing injection.

```php
// Valid usage — allowed
$db->whereIn('id', [1, 2, 3]);

// Invalid usage — throws exception or blocks
$db->whereIn('id', []);                // Empty array
$db->whereIn('id', ['1; DROP TABLE']); // Injection attempt if unescaped (blocked here)
```
In these cases, `whereIn()` will raise an error or reject the input, preventing unsafe SQL from being executed.

**NEW `whereNotIn()` and `orWhereNotIn()`**

These methods enable safe and parameterized exclusion filtering by sets in your SQL WHERE clauses.

```php
// Exclude users with ids 10, 20, 30
$db->whereNotIn('id', [10, 20, 30]);
$users = $db->get('users');

// Exclude users with status 'inactive' or 'banned'
$db->whereNotIn('status', ['inactive', 'banned'])
->orWhereNotIn('status', ['pending']);
$users = $db->get('users');
```
**What `whereNotIn()` blocks**

`whereNotIn()` blocks empty arrays and ensures all values are safely bound as parameters, preventing injection.

```php
// Valid usage — allowed
$db->whereNotIn('id', [10, 20, 30]);

// Invalid usage — throws exception or blocks
$db->whereNotIn('id', []);             // Empty array
$db->whereNotIn('id', ['DROP TABLE']); // Injection attempt if unescaped (blocked here)
```
In these cases, `whereNotIn()` will raise an error or reject the input, preventing unsafe SQL from being executed.

**NEW `whereIsNull()` and `orWhereIsNull()`**
These methods allow you to filter records where a specific column is NULL.

```php
// Users without a phone number
$db->whereIsNull('phone');
$users = $db->get('users');

// OR-condition with NULL check
$db->where('email', 'test@example.com')
->orWhereIsNull('email');
$users = $db->get('users');
```
**NEW `whereIsNotNull()` and `orWhereIsNotNull()`**
These methods filter records where a column is NOT NULL.

```php
// Users with a password set
$db->whereIsNotNull('password');
$users = $db->get('users');

// OR-condition combined
$db->where('active', 1)
->orWhereIsNotNull('last_login');
$users = $db->get('users');
```
>Notes: `whereIsNull()`,`orWhereIsNull()`,`whereIsNotNull()` and `orWhereIsNotNull()`
> - These methods do not accept a value argument – they simply append IS NULL or IS NOT NULL to the WHERE clause.
> - Works with all SQL-capable fields, including strings, numbers, and dates.
> - Safe to combine with other `where*()` and orWhere*() methods.

## Sanitization: `$db->escape()`

In most cases, you don't need to escape or quote values manually — **all values are automatically bound using prepared statements**. This protects your queries from SQL injection by design.

However, if you ever need to manually escape a value (e.g., for debugging or building custom dynamic SQL), you can use the `$db->escape()` method:

```php
$unsafe = "'; DROP TABLE users; --";
$safe = $db->escape($unsafe);
// -> returns: \'; DROP TABLE users; --
```

> ⚠️ Note: `$db->escape()` has **no functionality** — it is a **dummy method** by design.
> It does **not provide any protection** against SQL injection and should **not** be relied on for sanitization.
>
> If needed, you can enable real escaping by uncommenting the placeholder implementation inside the class,
> but this is **strongly discouraged**.
>
> ✅ When using this library as intended — via prepared statements and bindings — `escape()` is completely obsolete.
> All input is handled safely and efficiently without the need for manual escaping.

✅ Correct usage (safe):
```php
$db->where('name', $name); // safely bound and escaped internally
```
❌ Wrong usage (vulnerable):
```php
$name = $db->escape($_GET['name']);
$sql = "SELECT * FROM users WHERE name = '$name'";
$db->rawQuery($sql); // 🚨 vulnerable to mistakes
```
Always prefer bindings over manual escaping.
Use escape() only in edge cases like debugging, logging, or custom output formatting.

## Ordering method
You can sort the results using `orderBy()`:
```php
$db->orderBy("id", "asc");
$db->orderBy("login", "desc");
$db->orderBy("RAND()");
$results = $db->get('users');
// SELECT * FROM users ORDER BY id ASC, login DESC, RAND()
```

Order by specific values:
```php
$db->orderBy('userGroup', 'ASC', ['superuser', 'admin', 'users']);
$db->get('users');
// SELECT * FROM users ORDER BY FIELD(userGroup, 'superuser', 'admin', 'users') ASC
```

Using `setPrefix()`?
If you're working with table prefixes and using fully-qualified column names in `orderBy()`, make sure to wrap the table name in backticks:
```php
$db->setPrefix("t_");

$db->orderBy("users.id", "asc");
$results = $db->get('users');
// WRONG: SELECT * FROM t_users ORDER BY users.id ASC

$db->orderBy("`users`.id", "asc");
$results = $db->get('users');
// CORRECT: SELECT * FROM t_users ORDER BY t_users.id ASC
```

## Grouping method
```php
$db->groupBy ("name");
$results = $db->get ('users');
// Gives: SELECT * FROM users GROUP BY name;
```

### Properties sharing
It is also possible to copy properties

```php
$db->where("agentId", 10);
$db->where("active", true);

$customers = $db->copy();
$res = $customers->get("customers", [10, 10]);
// SELECT * FROM customers WHERE agentId = 10 AND active = 1 LIMIT 10, 10

$cnt = $db->getValue("customers", "count(id)");
echo "total records found: " . $cnt;
// SELECT count(id) FROM customers WHERE agentId = 10 AND active = 1
```

### Subqueries
You can build subqueries and use them in WHERE, JOIN, INSERT, and other SQL statements.
Subquery without alias (for use in INSERT, UPDATE, or WHERE):

```php
$sq = $db->subQuery();
$sq->get("users");
// (SELECT * FROM users)
```

Subquery with alias (for use in JOINs):
```php
$sq = $db->subQuery("sq");
$sq->get("users");
// (SELECT * FROM users) sq
```

Subquery in WHERE:
```php
$ids = $db->subQuery();
$ids->where("qty", 2, ">");
$ids->get("products", null, "userId");

$db->where("id", $ids, "IN");
$res = $db->get("users");
// SELECT * FROM users WHERE id IN (SELECT userId FROM products WHERE qty > 2)
```

Subquery in INSERT:
```php
$userIdQ = $db->subQuery();
$userIdQ->where("id", 6);
$userIdQ->getOne("users", "name");

$data = [
    "productName" => "test product",
    "userId"      => $userIdQ,
    "lastUpdated" => $db->now()
];

$id = $db->insert("products", $data);
// INSERT INTO products (productName, userId, lastUpdated)
// VALUES ("test product", (SELECT name FROM users WHERE id = 6), NOW());
```

Subquery in SELECTS:
```php
$ids = $db->subQuery ();
$ids->where ("qty", 2, ">");
$ids->get ("products", null, "userId");

$db->where ("id", $ids, 'in');
$res = $db->get ("users");
// Gives SELECT * FROM users WHERE id IN (SELECT userId FROM products WHERE qty > 2)
```


Subquery in JOIN:
```php
$usersQ = $db->subQuery("u");
$usersQ->where("active", 1);
$usersQ->get("users");

$db->join($usersQ, "p.userId=u.id", "LEFT");
$products = $db->get("products p", null, "u.login, p.productName");

print_r($products);
// SELECT u.login, p.productName FROM products p
// LEFT JOIN (SELECT * FROM t_users WHERE active = 1) u ON p.userId = u.id;
```

## JOINs and Join Conditions
The join() method allows you to perform SQL joins between tables. You can combine this with joinWhere() and joinOrWhere() to add conditional logic directly into the JOIN ON clause.

Basic JOIN Example
Join the products table with the users table using a LEFT JOIN on tenantID:
```php
$db->join("users u", "p.tenantID = u.tenantID", "LEFT");
$db->where("u.id", 6);
$products = $db->get("products p", null, "u.name, p.productName");

print_r($products);
// SELECT u.name, p.productName FROM products p
// LEFT JOIN users u ON p.tenantID = u.tenantID
// WHERE u.id = 6
```

Join Conditions (AND inside ON clause)
To add additional conditions within the JOIN, use joinWhere():
```php
$db->join("users u", "p.tenantID = u.tenantID", "LEFT");
$db->joinWhere("users u", "u.tenantID", 5);
$products = $db->get("products p", null, "u.name, p.productName");

print_r($products);
// SELECT u.name, p.productName FROM products p
// LEFT JOIN users u ON (p.tenantID = u.tenantID AND u.tenantID = 5)
```
🟢 joinWhere() targets the ON clause of the JOIN, not the global WHERE.

Join Conditions (OR inside ON clause)
To use OR logic within the JOIN ON clause, use joinOrWhere():
```php
$db->join("users u", "p.tenantID = u.tenantID", "LEFT");
$db->joinOrWhere("users u", "u.tenantID", 5);
$products = $db->get("products p", null, "u.name, p.productName");

print_r($products);
// SELECT u.name, p.productName FROM products p
// LEFT JOIN users u ON (p.tenantID = u.tenantID OR u.tenantID = 5)
```

Common Mistake: Mixing with where()
```php
$db->join("users u", "p.tenantID = u.tenantID", "LEFT");
$db->where("u.tenantID", 5); // ⛔ NOT in JOIN ON, but in WHERE clause
```
This puts the condition after the JOIN, which can lead to incorrect results in LEFT JOINs. Use joinWhere() if the condition should be part of the JOIN logic itself.
✅ Best Practice: Use joinWhere() and joinOrWhere() only when the condition must apply during the JOIN phase (i.e., in the ON clause), especially for LEFT JOIN, RIGHT JOIN, and OUTER JOIN scenarios.

## SQL Conditions (EXISTS, NOT EXISTS, HAS, ...)
You can use subqueries inside WHERE clauses by combining them with operators like EXISTS, NOT EXISTS, IN, NOT IN, etc.
This is useful when you need to check for the existence of related rows in another table.

**EXISTS condition**
```php
$sub = $db->subQuery();
$sub->where("status", "active");
$sub->get("sessions s", null, "s.userId");

$db->where(null, $sub, 'EXISTS');
$db->get("users");
// SELECT * FROM users WHERE EXISTS (SELECT s.userId FROM sessions s WHERE status = 'active')
```

**EXISTS condition**
```php
$sub = $db->subQuery();
$sub->where("status", "active");
$sub->get("sessions s", null, "s.userId");

$db->where(null, $sub, 'EXISTS');
$db->get("users");
// SELECT * FROM users WHERE EXISTS (SELECT s.userId FROM sessions s WHERE status = 'active')
```

**IN condition using subquery**
```php
$sub = $db->subQuery();
$sub->get("orders", null, "customerId");

$db->where("id", $sub, "IN");
$customers = $db->get("customers");
// SELECT * FROM customers WHERE id IN (SELECT customerId FROM orders)
```

**NOT IN condition using subquery**
```php
$sub = $db->subQuery();
$sub->get("orders", null, "customerId");

$db->where("id", $sub, "NOT IN");
$customers = $db->get("customers");
// SELECT * FROM customers WHERE id NOT IN (SELECT customerId FROM orders)
```
🟢 You can also use this pattern with multi-table JOINs or with aggregate conditions (e.g. COUNT, SUM) if supported by your SQL server.

## Lock / Unlock
You can manually lock one or more tables using the default lock mode (WRITE by default).

Set the lock type globally:
```php
$db->setLockMethod('WRITE'); // or 'READ'
```
Locking a single table:
```php
$db->lock('users');
// LOCK TABLES users WRITE;
```
Locking multiple tables:
```php
$db->lock(['users', 'logs']);
// LOCK TABLES users WRITE, logs WRITE;
```
Unlock all tables:
```php
$db->unlock();
// UNLOCK TABLES;
```
⚠️ Note: If an error occurs during a locked transaction, you are responsible for unlocking the tables manually.

**Coming Soon:** `unlockOnFailure()`
In a future release, the method unlockOnFailure() will automatically release table locks if an exception or failure is detected during query execution. This helps prevent deadlocks and stale locks in long-running processes.
```php
$db->lock('users WRITE');
$db->unlockOnFailure(); // ← not implemented yet
```

## Transaction
Transactions allow you to execute multiple queries with full rollback/commit control. This is especially useful for ensuring data integrity across dependent operations.

Transaction Helpers
⚠️ Transactions only work on InnoDB tables!

Basic example — rollback on failure:

```php
$db->startTransaction();

if (!$db->insert('myTable', $insertData)) {
    // Insert failed — rollback changes
    $db->rollback();
} else {
    // Everything ok — commit transaction
    $db->commit();
}
```

You can chain multiple statements inside the transaction block. If any one of them fails, you can safely roll back everything:

```php
$db->startTransaction();

try {
    $db->insert('table1', $data1);
    $db->insert('table2', $data2);
    $db->update('table3', $data3);

    $db->commit();
} catch (\Exception $e) {
    $db->rollback();
    echo "Transaction failed: " . $e->getMessage();
}
```

✅ You can also use unlockOnFailure() to automatically release table locks when something fails (see Locking section).

🛠️ Helper Methods

🔍 Debugging
You can control the internal debug behavior using:
```php
$db->debug(1);
```
➡️ Enables exception logging (e.g. file log or stdout, depending on implementation)

```php
$db->debug(2);
```
➡️ Enables deep debugging, which also outputs:

- Final SQL query
- Bound parameters (e.g. directly via echo during runtime)

Even without $db->debug(), you can still inspect the last error manually:

```php
echo $db->getLastError();   // Last error message
echo $db->getLastErrno();   // Last error code

$db->where('login', 'admin')->update('users', ['firstName' => 'Jack']);

if ($db->getLastErrno() === 0){
    echo 'Update succesfull';
} else {
    echo 'Update failed. Error: '. $db->getLastError();
}
```

## ⏱️ Query Execution Time Benchmarking
To measure how long each query takes, you can enable the trace mode using:

```php
$db->setTrace(true);
```
Optionally, provide a second argument to strip a common path prefix from file names in the trace output:
```php
$db->setTrace(true, $_SERVER['DOCUMENT_ROOT']);
```
Now run some queries:
```php
$db->get("users");
$db->get("test");
print_r($db->trace);
```
Trace output example:
```php
    [0] => Array
        (
            [0] => SELECT * FROM t_products WHERE active = 1
            [1] => 0.0021378993988037         // execution time in seconds
            [2] => PDOdb->get() >> file "/var/www/api/ProductController.php" line #78
        )

    [1] => Array
        (
            [0] => SELECT name, email FROM t_users LIMIT 5
            [1] => 0.00084209442138672
            [2] => PDOdb->get() >> file "/var/www/api/UserService.php" line #142
        )

    [2] => Array
        (
            [0] => INSERT INTO t_logs (event, createdAt) VALUES (?, NOW())
            [1] => 0.0013091564178467
            [2] => PDOdb->insert() >> file "/var/www/api/Logger.php" line #55
        )

```

## Special Thanks
This project was originally inspired by the excellent work of ThingEngineer and his legendary PHP MySQLi Database Class.
I’ve been a long-time user and fan of that class — and this rewrite in PDO is built with great respect for its simplicity and power.

However, times change — and so does PHP.
With this project, I aim to bring the same developer experience to modern PDO-based applications, with enhanced error handling, multi-connection support, subquery building, and full compatibility with legacy syntax.

Thanks again to ThingEngineer for all the years of solid groundwork. 🙏


## 📝 License / Lizenz


This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
