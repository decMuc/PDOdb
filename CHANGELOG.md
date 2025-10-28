# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)  
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---
## [1.3.7] – 2025-10-28
### Added
- `whereRaw()` and `orWhereRaw()`:
    - Allows injecting custom WHERE fragments (including complex grouped logic, nested AND/OR blocks, function calls, multi-column comparisons, etc.).
    - Supports `?` placeholders plus a value array for proper PDO prepared binding, e.g.  
      `$db->whereRaw("(base_name LIKE ? OR sku LIKE ?)", [$search, $search]);`
    - Can be combined with existing builder methods and will be included in `$db->get()`, `$db->update()`, `$db->delete()`, etc.

- Automatic decimal comma normalization for numeric columns:
    - When writing to `DECIMAL`, `FLOAT`, `DOUBLE`, etc., values like `"12,20"` are now automatically normalized to `"12.20"` internally.
    - Removes the need to manually replace commas with dots before insert/update.
    - Applies only to numeric column types; regular string/text fields are not touched.

### Changed
- Internal WHERE builder now supports raw segments alongside the typed helpers (`whereInt()`, `whereStr()`, …). Raw segments are appended without column/operator validation by design, but their parameters (if provided) are still bound via prepared statements.

### Notes
- Decimal normalization is considered non-breaking: existing code that already passes `"12.20"` continues to behave the same. The feature mainly removes repetitive pre-processing of localized numeric input.
## [1.3.6] – 2025-08-15
### Added
- Reuse of named scalar placeholders across multiple SQL occurrences.
- Deterministic expansion for named arrays: :name_{occIdx}_{elemIdx}.

### Changed
- Strict separation of named vs. positional placeholders.
- Clearer exceptions; guaranteed state reset on error.

### Fixed
- Accurate placeholder/parameter count validation for positional arrays.

### Breaking
- Empty arrays now throw InvalidArgumentException.
- Mixing named and positional placeholders throws InvalidArgumentException.
---
## [1.3.5] – 2025-07-20

### Added
- Internal query counter (`_queryCounter`) with automatic tracking via `countQuery()`
- Manual error registration via `setManualLastErrors()` with `_manualErrorSet` flag
- Batch mode flag (`_batchMode`) to suppress counter increments during multi-inserts
- Improved `getLastError()` and `getLastErrno()` to return error from latest counted query only

### Changed
- Refactored `handleException()` and all insert methods to unify error tracking
- `_buildInsert()` now handles counter increment internally (unless in batch mode)
- Reset logic now clears `_manualErrorSet` and `_batchMode` reliably (`reset(true)`)

### Fixed
- Prevented stale or incorrect query counter states after manual or multi-insert failures
- `paginate()` now resets state cleanly and no longer double-counts queries
---
## [1.3.4] – 2025-07-19

### Fixed
- Fixed `delete()` query bug causing `DELETE WHERE ...` without table name
- Resolved fatal exception when calling `where() + delete()`

### Added
- Full support for `[F]`, `[I]`, `[N]` placeholders in `insert()`, `update()`, `replace()`
- Subquery validation and alias extraction
- New central `_secureValidateInsertValues()` for value safety

### Changed
- `secureWhere()`: stronger validation (aggregates, suspicious input, function safety)
- Improved `setPageLimit()`, `getReturnKey()` – now instance-safe
- Query debug tracking (`_lastDebugQuery`) activated even on errors

### Internal
- Cleaned up `_buildQuery()` structure and reset behavior
- Improved logging via `logQuery()` and `logException()` in all query types

### ⚠️ Deprecated (to be removed in v1.5.0)

- `$db->pageLimit`  
  → use `setPageLimit()` instead (instance-safe)

- `$db->map`  
  → use `setReturnKey()` instead

- `$db->arrayBuilder()`, `$db->objectBuilder()`, `$db->jsonBuilder()`  
  → use `setOutputMode('array' | 'object' | 'json')` instead

- `$db->escape()`  
  → remains a non-functional dummy and may be removed in a future version

- Static global instance via `getInstance()`  
  → use named instances via `getInstance('your_name')` instead
---

## [1.3.3] – 2025-07-14

### Added

- **Heuristic WHERE condition checker** (enabled by default):  
  A new safeguard mechanism has been added to detect suspicious WHERE clause values based on simple heuristics.  
  This helps catch obvious SQL injection attempts (e.g. `1; DROP TABLE`, `SLEEP(1)`, etc.) even when using the base `where()` method.

### Config
- The heuristic check can be disabled globally via:

```php
  define('PDOdb_HEURISTIC_WHERE_CHECK', false);
```  
**Notes**
* This check adds a small performance cost (typically 1–3 ms per affected query), but significantly improves safety for legacy or user-controlled input.

## [1.3.2] – 2025-07-12
### Changed
- Reorganized method order across the entire class (grouped logically)
- secureHaving(): added support for alias/aggregate validation
- _buildCondition(): detects SQL functions and skips ticks accordingly
- HAVING clauses with invalid inputs now trigger exceptions, not fatal errors


[1.3.0] – 2025-07-08
### Added
Default instance name `$instance = 'default'` added to simplify multi-instance management.
This ensures consistent fallback behavior if no named instance is specified.

### Removed
Removed legacy methods `loadData()` and `loadXml()`.

These were inherited from the original MySQLi-based implementation (ThingEngineer),
but rely on `LOAD DATA INFILE` and `LOAD XML INFILE`, which are insecure, non-portable,
and fundamentally incompatible with a PDO-based database wrapper.

Developers should use native PHP CSV/XML parsing with prepared INSERT statements instead.

## [1.2.0] – 2025-07-03

### Added
- Full SQL expression validation for `groupBy()` and `orderBy()` methods
- Security filter: Blocks dangerous SQL constructs (e.g. `UNION`, `DROP`, `SLEEP`, etc.)
- Expression parser:
    - Nested function support (e.g. `ROUND(ABS(price), 2)`)
    - CASE WHEN validation with deep inspection
    - Literal and operator detection (e.g. `total > 100`)
- Extensive test coverage for allowed and blocked payloads

### Changed
- `validateSqlExpression()` is now used internally by `groupBy()` and `orderBy()` to enforce safe syntax
- Removed internal `$this->reset()` call to avoid silent data loss between method calls

### Fixed
- Fixed missing exception in CASE WHEN expressions with invalid inner content
- Proper handling of blocked `ORDER BY RAND()`, `SLEEP()`, `DROP TABLE`, and similar injection attempts


## [1.1.0] – 2025-07-03
### Added
- `whereInt()` / `orWhereInt()` for strict integer filtering
- `whereFloat()` / `orWhereFloat()` for safe float/decimal filtering
- `whereString()` / `orWhereString()` to block unsafe strings or injection attempts
- `whereBool()` / `orWhereBool()` with strict `true|false|1|0` validation
- `whereIsNull()` / `orWhereIsNull()` for `IS NULL` conditions
- `whereIsNotNull()` / `orWhereIsNotNull()` for `IS NOT NULL` conditions
- `whereIn()` / `orWhereIn()` using parameterized `IN (...)` clauses
- `whereNotIn()` / `orWhereNotIn()` using parameterized `NOT IN (...)` clauses

### Changed
- Bumped version from `v1.0.3` to `v1.1.0`
- Updated `README.md` with usage examples and behavior notes for all new methods