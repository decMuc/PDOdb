# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)  
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

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