# PDOdb – A Secure, Powerful MySQL Engine for PDO (PHP 8+)

PDOdb is a modern database wrapper and SQL builder for PHP 8+, built on top of PDO.  
It offers full support for secure query building, prepared statements, dynamic joins, subqueries, transactions, and strict validation.

🔒 Safe by default.  
⚙️ Fast, clean, extendable.  
📘 Documentation:  
👉 https://DocsPDOdb.decmuc.dev/

---

## Features

- Intuitive query builder (`get()`, `insert()`, `update()`, `delete()`)
- Smart `where*()` methods with type validation
- Nested queries, joins, subqueries, aliases
- Safe ORDER/GROUP clause parsing with heuristics
- Locking, transactions, pagination, bulk insert
- Inspired by [ThingEngineer/MySQLiDB](https://github.com/ThingEngineer/MysqliDb), but built for PDO with a similar API and strict SQL injection protection.

---

## Installation

```bash
composer require decmuc/pdodb
```

---

## Quick Example
Two lines for 90% of queries. MySQL-first, prepared by default—no manual prepare() or binding.
```php
$db->whereInt('id', 42);
$user = $db->getOne('users');

// Booleans made easy
$db->whereBool('status', true);
$users = $db->get('users');
// In short:
$users = $db->whereBool('status', true)->get('users');
// whereBool('status', 1) + get('users') is equivalent to
// SELECT * FROM users WHERE status = 1.
// false → 0 (or FALSE)
$users = $db->whereBool('status', false)->get('users');
// SELECT * FROM `users` WHERE `status` = 0

// Chaining with other conditions
$users = $db->whereBool('is_active', true)
   ->where('role', 'admin')
   ->orderBy('created_at', 'DESC')
   ->get('users');


```
For comprehensive documentation and more examples, visit the PDOdb docs: https://DocsPDOdb.decmuc.dev

---

## 📝 License / Lizenz


This project is licensed under the MIT License. See [LICENSE](LICENSE) for details
