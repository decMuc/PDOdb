# PDOdb – A Secure, Powerful MySQL Engine for PDO (PHP 8+)

PDOdb is a modern database wrapper and SQL builder for PHP 8+, built on top of PDO.  
It offers full support for secure query building, prepared statements, dynamic joins, subqueries, transactions, and strict validation.

🔒 Safe by default.  
⚙️ Fast, clean, extendable.  
📘 Documentation:  
👉 https://decmuc.github.io/DocsPDOdb/

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

```php
$db->whereInt('id', 42);
$user = $db->getOne('users');
```

---

## License

MIT © [decmuc](https://github.com/decmuc)
