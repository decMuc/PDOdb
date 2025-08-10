<?php
/**
 * PDOdb Class – ThingEngineer compatible PDO wrapper
 *
 * @category  Database Access
 * @package   decMuc\PDOdb
 * @author    L. Fischer
 * @copyright Copyright (c) 2025 L. Fischer
 * @license   https://opensource.org/licenses/MIT MIT License
 * @link      https://github.com/decMuc/PDOdb
 * @version   1.3.5
 * @inspired-by https://github.com/ThingEngineer/PHP-MySQLi-Database-Class
 */


namespace decMuc\PDOdb;

/**
 * Enables heuristic evaluation of suspicious WHERE values.
 *
 * When FALSE:
 *   → Values like "1; DROP TABLE x" or "1' OR 1=1" are immediately flagged
 *     as dangerous and blocked (exception thrown, query not executed).
 *
 * When TRUE (default):
 *   → Suspicious values are initially allowed, then checked against the
 *     column's expected data type (e.g., INT, VARCHAR).
 *     If a mismatch or clear injection attempt is detected, the query is blocked.
 *
 * Note: Setting this to TRUE introduces a small performance overhead,
 *       as column definitions may need to be retrieved via `getColumnMeta()`.
 */
if (!defined('PDOdb_HEURISTIC_WHERE_CHECK')) {
    define('PDOdb_HEURISTIC_WHERE_CHECK', true);
}
final class PDOdb
{

    // ==[ 01. Connection Management ]==
    protected static array $_instances = [];                           // All active DB instances
    protected array $connectionsSettings = [];                         // Connection config per instance
    protected string $defConnectionName = 'default';                   // Active instance name
    protected int $autoReconnectCount = 0;
    public bool $autoReconnect = true;
    protected array $pdoConnections = [];                              // PDO handles per instance
    protected array $_pdo = [];                                        // Legacy internal PDO handles
    protected $pdo;                                                    // Direct handle (current)

    // ==[ 02. Public Output & Helpers ]==

    public array $trace = [];                                          // Query traces (if enabled)
    protected array $_returnType = [];                                 // Output format: array, object, json
    protected array $_returnKey = [];                                  // Result mapping key
    protected bool $traceEnabled = false;                              // Enable query tracing
    protected float $traceStartQ = 0.0;                                // Start time for query trace
    protected string $traceStripPrefix = '';                           // Optional prefix for traces

    // ==[ 03. SQL Clauses & Conditions ]==

    protected array $_where = [];                                      // WHERE clauses
    protected array $_join = [];                                       // JOIN statements
    protected array $_joinAnd = [];                                    // ANDs after JOIN
    protected array $_joinWheres = [];                                 // WHEREs inside JOIN
    protected array $_pendingJoins = [];                               // Deferred JOINs
    protected array $_joinAliases = [];                                 // JOIN Aliases
    protected array $_groupBy = [];                                    // GROUP BY fields
    protected array $_orderBy = [];                                    // ORDER BY fields
    protected bool $_nestJoin = false;                                 // Nest results by joined table
    protected array $_having = [];                                     // HAVING clauses
    protected array $_pendingHaving = [];                              // Deferred HAVING
    protected array $_selectAliases = [];                              // Aliases from SELECT (for HAVING)

    // ==[ 04. SQL Query Building ]==

    protected ?string $_query = null;                                  // Active query string
    protected ?string $_lastQuery = null;                              // Last executed query
    protected string $_lastDebugQuery = '';                            // Debug variant of last query
    protected array $_queryOptions = [];                               // Query flags/options
    protected array $_bindParams = [];                                 // Bindings for prepared statements
    protected ?array $_updateColumns = null;                           // ON DUPLICATE KEY UPDATE columns

    protected bool $_forUpdate = false;

    // ==[ 05. Query Helpers & Meta ]==

    protected bool $isSubQuery = false;                                // Is subQuery builder?
    protected array $_columnCache = [];                                // Column type cache per table

    protected array $_checkBulkInput = [];                             // Check flag per connection
    // ==[ 06. Errors, Locks & Transactions ]==

    protected ?string $_stmtError = null;                              // Last error message
    protected ?string $_stmtErrno = null;                              // Last error code
    protected array $_errorStore = [];                                  // Error storage
    protected array $_manualErrorSet = [];
    protected array $_batchMode = [];
    protected array $_queryCounter = [];                                // Query error counter
    protected $_lastInsertId = null;                                   // Last inserted ID
    protected array $_tableLocks = [];                                 // Locked tables
    protected array $_tableLockMethod = [];                       // Lock type: READ or WRITE
    protected array $_transaction_in_progress = [];                   // Is transaction active?
    protected bool $_lockInShareMode = false;
    // ==[ 07. Pagination Support ]==

    protected array $_pageLimit = [];                                  // Page limits per instance
    public int $pageLimit = 20;                                        // ⛔ deprecated – use setPageLimit()
    protected array $_oldPlWasSet = [];                                // Was $pageLimit set directly?
    protected array $_newPlWasSet = [];                                // Was setPageLimit() used?
    // ==[ 08. Legacy Result Statistics ]==
    protected int $count = 0;                                           // ⛔ deprecated – use getRowCount() or getCount()
    protected int $totalCount = 0;                                      // ⛔ deprecated – use getTotalCount()
    protected int $totalPages = 0;                                      // ⛔ deprecated – use getTotalPages()

    // ==[ 09. Internal Result State ]==
    protected int $_count = 0;
    protected int $_totalCount = 0;
    protected int $_totalPages = 0;

    // ==[ 09. Debugging & Logging ]==

    protected array $_debugLevel = [];                                 // Debug level per instance

    // ==[ 10. Table Info & Prefixing ]==

    private array $prefix = [];                                        // Table prefix per instance
    private string $_tableName = '';                                   // Table name (with prefix)
    protected ?string $_tableAlias = null;                             // Optional table alias


    /**
     * @throws \Throwable
     */
    public function __construct(
        $host = null,
        $username = null,
        $password = null,
        $db = null,
        $port = 3306,
        $charset = 'utf8mb4',
        $socket = null,
        $instance = 'default'
    ) {
        $isSubQuery = false;

        if (is_array($host)) {
            foreach ($host as $key => $val) {
                $$key = $val;
            }
        }
        $this->defConnectionName       = $instance;

        // Set standards for instances
        $this->_debugLevel[$this->defConnectionName] = 0;
        $this->_pageLimit[$this->defConnectionName] = 20;
        $this->_oldPlWasSet[$this->defConnectionName] = false;
        $this->_newPlWasSet[$this->defConnectionName] = false;
        $this->_returnType[$this->defConnectionName] = 'array';
        $this->_returnKey[$this->defConnectionName] = null;
        $this->_checkBulkInput[$this->defConnectionName] = null;
        $this->_tableLocks[$this->defConnectionName] = [];
        $this->_tableLockMethod[$this->defConnectionName] = 'READ';
        $this->_transaction_in_progress[$this->defConnectionName] = false;
        $this->_errorStore[$this->defConnectionName] = null;
        $this->_queryCounter[$this->defConnectionName] = 0;
        $this->_manualErrorSet[$this->defConnectionName] = false;
        $this->_batchMode[$this->defConnectionName] = false;
        // addConnection wird IMMER aufgerufen – auch bei SubQuery
        $this->addConnection($instance, [
            'host'     => $host,
            'username' => $username,
            'password' => $password,
            'db'       => $db,
            'port'     => $port,
            'socket'   => $socket,
            'charset'  => $charset
        ]);

        // Erst danach wird geprüft, ob es sich um eine Subquery handelt aus $$key = $val; wenn host Alias oder leer ist
        if ($isSubQuery) {
            $this->isSubQuery = true;
            return;
        }

        if (isset($prefix)) {
            $this->setPrefix($prefix);
        }

        self::$_instances[$instance] = $this;
    }

    // Connects public

    /**
     * Registers a named database connection configuration without connecting immediately.
     *
     * This method supports lazy connection handling. You can define multiple profiles
     * and switch between them using connection('name').
     *
     * Valid configuration keys:
     * - host (string|null)
     * - username (string|null)
     * - password (string|null)
     * - db (string|null)
     * - port (int|null)
     * - socket (string|null)
     * - charset (string|null, default: 'utf8mb4')
     *
     * @param string $name Unique connection profile name (e.g. 'default', 'readonly')
     * @param array $params Associative array of connection parameters
     * @return self
     */
    public function addConnection(string $name, array $params): self
    {
        $defaults = [
            'host'     => null,
            'username' => null,
            'password' => null,
            'db'       => null,
            'port'     => null,
            'socket'   => null,
            'charset'  => 'utf8mb4',
        ];

        $this->connectionsSettings[$name] = array_merge($defaults, $params);

        return $this;
    }

    /**
     * Switches the active connection to another registered connection profile.
     *
     * This allows executing queries on different configured databases by name.
     *
     * @param string $name The name of the connection profile to use
     * @return self Fluent self for chaining
     * @throws \InvalidArgumentException If the connection profile does not exist
     */
    public function connection(string $name): self
    {
        if (!isset($this->connectionsSettings[$name])) {
            throw new \InvalidArgumentException("Connection '$name' was not added.");
        }

        $clone = clone $this;
        $clone->defConnectionName = $name;
        $clone->pdo = $clone->connect($name);
        // $clone->prefix = $this->connectionsSettings[$name]['prefix'] ?? '';

        return $clone;
    }

    /**
     * Closes the active PDO connection for the specified connection profile.
     *
     * This does not affect the stored connection configuration set via addConnection().
     * A subsequent call to connect() will re-establish the connection.
     *
     * @param string $connectionName Name of the connection profile (default: 'default')
     * @return void
     */
    public function disconnect(string $connectionName = 'default'): void
    {
        if (isset($this->_pdo[$connectionName])) {
            $this->_pdo[$connectionName] = null;
            unset($this->_pdo[$connectionName]);
        }
    }

    /**
     * Closes all currently active PDO connections for all registered profiles.
     *
     * Does not remove connection configurations (see addConnection).
     *
     * @return void
     */
    public function disconnectAll(): void
    {
        foreach (array_keys($this->_pdo) as $k) {
            $this->disconnect($k);
        }
    }

    /**
     * Returns the most recently created instance of the database handler.
     *
     * This allows access to the current DB context from static scope
     * after it was initialized somewhere else.
     *
     * @return self|null The last initialized instance or null if none exists
     */
    public static function getInstance(?string $name = null): ?self
    {
        return self::$_instances[$name ?? 'default'] ?? null;
    }

    /**
     * Checks if the current database connection is alive.
     *
     * @return bool True if connection is alive, false otherwise.
     */
    public function ping(): bool
    {
        try {
            $pdo = $this->connect($this->defConnectionName);
            return (bool) $pdo->query('SELECT 1');
        } catch (\PDOException) {
            // Optional: Log error if needed
            return false;
        }
    }

    /**
     * Sets the table prefix used in all queries.
     *
     * This allows dynamic adjustment of the prefix at runtime,
     * useful for multi-tenant setups or prefixed table schemas.
     *
     * @param string $prefix The prefix to prepend to table names.
     * @throws \InvalidArgumentException|\Throwable If the prefix contains invalid characters.
     */
    public function setPrefix(string $prefix): void
    {
        if ($this->isSubQuery) {
            $this->reset(true);
            throw $this->handleException(
                new \LogicException("Subqueries cannot change the prefix. Use full table names instead."),
                'setPrefix'
            );
        }

        if (!preg_match('/^[a-zA-Z0-9_]*$/', $prefix)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid prefix: '{$prefix}'. Only letters, numbers and underscores are allowed."),
                'setPrefix'
            );
        }

        $this->prefix[$this->defConnectionName] = $prefix;
    }

    protected function getPrefix(): string
    {
        return $this->prefix[$this->defConnectionName] ?? '';
    }

    // Connects (protected)

    /**
     * Establishes and returns a PDO connection using the specified connection profile.
     *
     * If the connection has not yet been established, it will be created using
     * the configuration defined in `addConnection()`. Supports standard host/port
     * as well as Unix socket-based connections.
     *
     * Example connection config structure:
     * [
     *   'host'     => 'localhost',
     *   'port'     => 3306,
     *   'socket'   => '/var/run/mysqld/mysqld.sock',
     *   'username' => 'root',
     *   'password' => 'secret',
     *   'db'       => 'test',
     *   'charset'  => 'utf8mb4'
     * ]
     *
     * @param string $connectionName Connection profile name (default: 'default')
     * @return \PDO The PDO instance for the requested connection
     * @throws \InvalidArgumentException If the specified connection profile does not exist
     * @throws \PDOException On connection failure
     */

    protected function connect(string $connectionName = 'default'): \PDO
    {
        if ($this->isSubQuery) {
            $this->reset(true);
            throw $this->handleException(
                new \LogicException("Subquery object must not initiate a DB connection."),
                'connect'
            );
        }

        if (!isset($this->connectionsSettings[$connectionName])) {
            throw new \InvalidArgumentException("Connection '$connectionName' has not been configured.");
        }

        if (!isset($this->_pdo[$connectionName])) {
            $cfg = $this->connectionsSettings[$connectionName];

            $dsn = "mysql:";
            $dsn .= isset($cfg['socket']) && $cfg['socket']
                ? "unix_socket={$cfg['socket']};"
                : "host={$cfg['host']};";

            if (!empty($cfg['port'])) {
                $dsn .= "port={$cfg['port']};";
            }

            $dsn .= "dbname={$cfg['db']};";

            if (!empty($cfg['charset'])) {
                $dsn .= "charset={$cfg['charset']}";
            }

            $this->_pdo[$connectionName] = new \PDO(
                $dsn,
                $cfg['username'] ?? null,
                $cfg['password'] ?? null,
                [
                    \PDO::ATTR_ERRMODE            => \PDO::ERRMODE_EXCEPTION,
                    \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
                    \PDO::ATTR_EMULATE_PREPARES   => false, // <-- NEU
                ]
            );
        }

        return $this->_pdo[$connectionName];
    }

    /**
     * Determines the PDO fetch mode based on the current return type setting.
     *
     * Used internally when executing queries to control the format of result rows.
     *
     * - 'array' → PDO::FETCH_ASSOC (default, associative arrays)
     * - 'object' → PDO::FETCH_OBJ (returns stdClass objects)
     * - 'json' → also uses FETCH_ASSOC (converted to JSON later)
     * - any other → PDO::FETCH_BOTH
     *
     * @return int One of the PDO::FETCH_* constants
     */
    protected function getPdoFetchMode(): int
    {
        return match ($this->getReturnMode()) {
            'array', 'json' => \PDO::FETCH_ASSOC,
            'object'        => \PDO::FETCH_OBJ,
            default         => \PDO::FETCH_BOTH
        };
    }

    /**
     * Returns the active PDO instance for the currently selected connection profile.
     *
     * If the connection has not yet been established, it will be opened automatically.
     *
     * @return \PDO The PDO instance
     * @throws \InvalidArgumentException If the connection configuration is missing
     * @throws \PDOException On connection failure
     */
    protected function pdo(): \PDO
    {
        if (!isset($this->_pdo[$this->defConnectionName])) {
            $this->connect($this->defConnectionName);
        }
        return $this->_pdo[$this->defConnectionName];
    }

    // Result & Context Helpers

    /**
     * Sets the return type to array for subsequent queries.
     *
     * @return self
     */
    public function arrayBuilder(): self
    {
        return $this->setOutputMode('array');
    }

    /**
     * Sets or gets the current debug level for this instance.
     *
     * Levels:
     *   0 = silent (no debug output)
     *   1 = log errors only
     *   2 = log errors + last built query
     *   3 = full debug (includes exceptions, logs, traces)
     *
     * @param int|null $level Set a new debug level (0–3), or null to get the current one.
     * @return int Current debug level
     */
    public function debug(?int $level = null): int
    {
        if ($level !== null) {
            // Clamp value to range 0–3
            $level = max(0, min(3, $level));
            $this->_debugLevel[$this->defConnectionName] = $level;
        }

        return $this->_debugLevel[$this->defConnectionName] ?? 0;
    }

    /**
     * Dummy escape method for compatibility.
     * Use prepared statements and parameter binding instead!
     *
     * @param string $str Input string.
     * @return string The input string untouched.
     * @deprecated Will be removed in v1.5.x – use rawQuery() instead.
     */
    public function escape(string $str): string
    {
        // Could use addslashes($str) but better to encourage prepared statements
        return $str;
    }

    /*
     * If you want a real escape, uncomment the method below:
     *
     * public function escape(string $str): string
     * {
     *     $pdo = $this->connect($this->defConnectionName);
     *     $quoted = $pdo->quote($str);
     *     // PDO::quote() returns the string with quotes, e.g. 'abc', remove them:
     *     return substr($quoted, 1, -1);
     * }
     */

    /**
     * Enables or disables strict validation for insertBulk().
     *
     * If enabled (true), the insertBulk() method will reject any input rows that contain:
     * - Special SQL markers like [F], [I], [N]
     * - Subquery objects (implementing getSubQuery())
     *
     * This is disabled by default for performance reasons.
     * Enable only if you expect dynamic or user-provided values.
     *
     * @param bool $state True to enable, false to disable
     */
    public function enableBulkCheck(bool $state = true): void
    {
        $this->_checkBulkInput[$this->defConnectionName] = $state;
    }

    public function getTableName(): string
    {
        return $this->_tableName;
    }

    public function getTableAlias(): ?string
    {
        return $this->_tableAlias;
    }

    /**
     * Returns the last executed SQL query with bound parameters interpolated.
     *
     * Useful for debugging purposes. Do not use the returned string for execution.
     *
     * @return string The last executed SQL query as a string.
     */
    public function getLastQuery(): string
    {
        $sql = $this->_lastQuery ?? $this->_query;
        $this->_lastQuery = null;
        if (!is_string($sql) || trim($sql) === '') {
            return '-- Query not available: possibly blocked by exception or security rule';
        }
        return $sql;
    }

    /**
     * Returns the last error message from the last executed statement or connection.
     *
     * @return null|string Error message, or 'No error' if none occurred.
     */
    public function getLastError(): null|string
    {
        $conn = $this->defConnectionName;
        $cnt  = $this->_queryCounter[$conn] ?? 0;

        if (isset($this->_errorStore[$conn]) && $this->_errorStore[$conn]['count'] === $cnt){
            return $this->_errorStore[$conn]['error'] ?? null;
        }
        return null;
    }

    /**
     * Returns the last error code from the executed PDO statement.
     *
     * @return int|null PDO error code, or 0 if none.
     */
    public function getLastErrno(): null|int
    {
        $conn = $this->defConnectionName;
        $cnt  = $this->_queryCounter[$conn] ?? 0;

        if (isset($this->_errorStore[$conn]) && $this->_errorStore[$conn]['count'] === $cnt){
            return $this->_errorStore[$conn]['code'] ?? null;
        }
        return null;
    }

    /**
     * Returns the ID of the last inserted row.
     *
     * This method returns the last auto-increment ID generated by the database connection.
     * If the database driver does not support this feature or the call fails, an exception is thrown.
     *
     * @return string The last inserted ID as a string (even for numeric auto-increments).
     * @throws \Throwable If the underlying PDO call fails.
     */
    public function getInsertId(): string
    {
        try {
            $pdo = $this->connect($this->defConnectionName);
            return $pdo->lastInsertId();
        } catch (\Throwable $e) {
            $this->handleException($e, 'getInsertID');
        }
    }

    /**
     * Returns the number of rows from the last SELECT/GET operation.
     * Short Alias to getRowCount()
     *
     * @return int|null  Number of rows, or null if no query has run.
     */
    public function getCount(): ?int
    {
        return $this->_count;
    }

    /**
     * Returns the number of rows from the last SELECT/GET operation.
     *
     * @return int|null  Number of rows, or null if no query has run.
     */
    public function getRowCount(): ?int
    {
        return $this->getCount();
    }

    /**
     * Returns the total number of matching rows from the last paginated query.
     *
     * @return int|null  Total row count, or null if paginate() was not used.
     */
    public function getTotalCount(): ?int
    {
        return $this->_totalCount;
    }

    /**
     * Returns the total number of pages from the last paginated query.
     *
     * @return int|null  Total pages, or null if paginate() was not used.
     */
    public function getTotalPages(): ?int
    {
        return $this->_totalPages;
    }
    /**
     * Checks whether this instance represents a subquery context.
     *
     * @return bool True if this is a subquery, false otherwise
     */
    public function isSubQuery(): bool
    {
        return $this->isSubQuery;
    }

    /**
     * Sets the return type to JSON for subsequent queries.
     *
     * @return self Returns the current instance for chaining.
     */
    public function jsonBuilder(): self
    {
        return $this->setOutputMode('json');
    }

    /**
     * Configures the query to return results as an associative array indexed by the given field.
     *
     * Example:
     *   $db->map('id')->get('users');
     *   // returns: [1 => [...], 2 => [...], ...]
     *
     * This is an alias for setReturnKey().
     *
     * @param string $idField The field name to use as array key in result set.
     * @return self
     * @deprecated Will be removed in v1.5.x – use rawQuery() instead.
     */
    public function map(string $idField): self
    {
        return $this->setReturnKey($idField);
    }

    /**
     * Sets the return type to object for subsequent queries.
     *
     * @return self
     */
    public function objectBuilder(): self
    {
        return $this->setOutputMode('object');
    }

    /**
     * Store columns for ON DUPLICATE KEY UPDATE clause and optionally last insert ID.
     *
     * @param array       $updateColumns Columns to update on duplicate key.
     * @param int|string|null $lastInsertId Optional last insert ID to use.
     *
     * @return self Fluent interface.
     */
    public function onDuplicate(array $updateColumns, int|string|null $lastInsertId = null): self
    {
        $this->_lastInsertId = $lastInsertId;
        $this->_updateColumns = $updateColumns;
        return $this;
    }

    /**
     * Alias for setReturnKey(), for compatibility/nicer chaining.
     *
     * @param string $field
     * @return self
     */
    public function returnKey(string $field): self
    {
        return $this->setReturnKey($field);
    }

    /**
     * Set a column name to be used as key for result-mapping.
     *
     * @param string|null $key Column name or null to disable.
     * @return self
     */
    public function setMapKey(?string $key): self
    {
        return $this->setReturnKey($key);
    }

    /**
     * Sets the output format for SELECT results of this connection.
     *
     * Valid modes are:
     * - 'array' (default): returns associative arrays
     * - 'object': returns stdClass objects
     * - 'json': prepares array for later JSON encoding
     *
     * @param string $mode Output mode: 'array', 'object' or 'json' (case-insensitive)
     * @return self
     */
    public function setOutputMode(string $mode): self
    {
        $mode = strtolower(trim($mode));
        switch ($mode) {
            case 'json':
            case 'object':
            case 'array':
                $this->_returnType[$this->defConnectionName] = $mode;
                break;
            default:
                $this->_returnType[$this->defConnectionName] = 'array';
        }
        return $this;
    }

    /**
     * Returns the current output mode for this connection.
     *
     * @return string 'array', 'object' or 'json'
     */
    protected function getReturnMode(): string
    {
        return $this->_returnType[$this->defConnectionName] ?? 'array';
    }

    /**
     * Sets the page limit for pagination on the current connection instance.
     *
     * This replaces the legacy $pageLimit property with an instance-safe value.
     *
     * @param int $limit A positive integer greater than zero.
     *
     * @throws \InvalidArgumentException|\Throwable If the value is less than 1.
     */
    public function setPageLimit(int $limit): void
    {
        if ($limit < 1) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException('Page limit must be greater than zero.'),
                'setPageLimit'
            );
        }

        $this->_pageLimit[$this->defConnectionName] = $limit;
        $this->_newPlWasSet[$this->defConnectionName] = true;
    }

    protected function getPageLimit(): int
    {
        $limit = $this->_pageLimit[$this->defConnectionName] ?? 20;

        if (
            !($this->_newPlWasSet[$this->defConnectionName] ?? false)
            && !($this->_oldPlWasSet[$this->defConnectionName] ?? false)
        ) {
            $this->logNotice(
                "Notice: No setPageLimit() or \$pageLimit call on instance '{$this->defConnectionName}'. "
                . "Using fallback value ($limit). This behavior will be removed in v1.5.x"
            );
        }

        return $limit;
    }
    /**
     * Sets the key for mapping result arrays.
     * If enabled, results will be mapped like [$keyValue => $row, ...]
     *
     * @param string|null $key Column name, or null to disable mapping.
     * @return self
     */
    public function setReturnKey(?string $key): self
    {
        $this->_returnKey[$this->defConnectionName] = $key;
        return $this;
    }

    /**
     * Returns the current output mode for this connection.
     *
     * @return null|string 'key'
     */
    protected function getReturnKey(): null|string
    {
        return $this->_returnKey[$this->defConnectionName] ?? null;
    }

    /**
     * Enables or disables SQL query execution time tracking.
     *
     * When enabled, the library will internally record query durations
     * and optionally strip absolute paths from trace logs.
     *
     * @param bool   $enabled     Whether to enable execution tracing.
     * @param string $stripPrefix Optional path prefix to strip from trace logs.
     * @return static             Returns current instance for chaining.
     */
    public function setTrace(bool $enabled, string $stripPrefix = ''): self
    {
        $this->traceEnabled = $enabled;
        $this->traceStripPrefix = $stripPrefix;
        return $this;
    }

    /**
     * Enables SQL_CALC_FOUND_ROWS for the next SELECT query,
     * allowing retrieval of the total number of matching rows (ignoring LIMIT).
     *
     * Usage:
     *   $users = $db->withTotalCount()->get('users', [$offset, $count]);
     *   echo $db->totalCount;
     *
     * @return self
     */
    public function withTotalCount(): self
    {
        $this->setQueryOption('SQL_CALC_FOUND_ROWS');
        return $this;
    }

    // Public SQL Expression Helpers

    /**
     * Returns the current date, datetime, or timestamp depending on mode.
     *
     * @param int $mode 0 = Y-m-d (default), 1 = Y-m-d H:i:s, 3 = UNIX timestamp
     * @return string|int
     */
    public function currentDate(int $mode = 0): string|int
    {
        return match ($mode) {
            0 => date('Y-m-d'),
            1 => date('Y-m-d H:i:s'),
            3 => time(),
            default => date('Y-m-d'),
        };
    }

    // Alias for currentDate Method

    public function currentDateOnly(): string
    {
        return $this->currentDate(0);
    }

    public function currentDatetime(): string
    {
        return $this->currentDate(1);
    }

    public function currentTimestamp(): int
    {
        return $this->currentDate(3);
    }

    /**
     * Returns an SQL expression to decrement a numeric field.
     *
     * Can be used in an update operation like:
     *   $db->update('table', ['stock' => $db->dec(1)]);
     *   → Generates: `stock = stock - 1`
     *
     * @param int|float $num Amount to decrement by (default: 1)
     * @return array<string, string> SQL-compatible expression
     * @throws \InvalidArgumentException|\Throwable
     */
    public function dec(int|float $num = 1): array
    {
        if (!is_numeric($num)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException('Argument supplied to dec() must be numeric.'),
                'dec'
            );
        }

        return ['[I]' => ' - ' . $num];
    }

    /**
     * Generates a SQL expression for a user-defined function call.
     *
     * This allows raw SQL expressions (e.g., "CONCAT(?, ?)") to be used in
     * INSERT/UPDATE statements, along with optional bound parameters.
     *
     * Example:
     *   $db->update('users', [
     *       'fullName' => $db->func('CONCAT(?, ?)', [$firstName, $lastName])
     *   ]);
     *   → Generates: `fullName = CONCAT(?, ?)` with bound values
     *
     * @param string     $expr       SQL expression (e.g. "NOW()", "CONCAT(?, ?)")
     * @param array|null $bindParams Optional array of parameters to bind
     * @return array<string, array{0: string, 1?: array}>
     */
    public function func(string $expr, ?array $bindParams = null): array
    {
        return ['[F]' => [$expr, $bindParams]];
    }

    /**
     * Returns an SQL-compatible expression to increment a numeric field.
     *
     * Usage in an update statement:
     *   $db->update('table', ['views' => $db->inc(1)]);
     *   → Results in SQL: `views = views + 1`
     *
     * @param int|float $num Amount to increment by (default: 1)
     * @return array<string, string> SQL-compatible update expression
     * @throws \InvalidArgumentException|\Throwable If the value is not numeric.
     */
    public function inc(int|float $num = 1): array
    {
        if (!is_numeric($num)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException('Argument supplied to inc() must be numeric.'),
                'inc'
            );
        }

        return ['[I]' => ' + ' . $num];
    }

    /**
     * Generates a SQL-compatible interval expression for use in WHERE clauses or arithmetic expressions.
     *
     * Supports both simple and compound intervals using shorthand units:
     *   's' = second, 'm' = minute, 'h' = hour,
     *   'd' = day,    'M' = month,  'Y' = year
     *
     * Examples:
     *   $this->interval('-1d')            returns: NOW() - interval 1 day
     *   $this->interval('10m', 'ts')      returns: ts + interval 10 minute
     *   $this->interval('1Y2M3d')         returns: NOW() + interval 1 year + interval 2 month + interval 3 day
     *   $this->interval('-5m30s')         returns: NOW() - interval 5 minute - interval 30 second
     *
     * @param string $diff Interval definition, e.g. "10m", "-1d", "1Y2d", "5m30s"
     * @param string $func Base SQL function or column name (default: NOW())
     * @return string SQL fragment such as "NOW() + interval 10 minute"
     * @throws \Exception|\Throwable If the format or unit is invalid
     */

    public function interval(string $diff, string $func = 'NOW()'): string
    {
        $types = [
            's' => 'second',
            'm' => 'minute',
            'h' => 'hour',
            'd' => 'day',
            'M' => 'month',
            'Y' => 'year',
        ];

        if ($diff !== '' && preg_match('/([+-]?)\s*([0-9]+)\s*([a-zA-Z]?)/', $diff, $matches)) {
            $incr = $matches[1] ?: '+';
            $amount = $matches[2] ?? '';
            $unit = $matches[3] ?: 'd';

            if (!isset($types[$unit])) {
                $this->reset(true);
                throw $this->handleException(
                    new \Exception("Invalid interval unit '{$unit}' in '{$diff}'"),
                    'interval'
                );
            }

            if ($amount === '') {
                $this->reset(true);
                throw $this->handleException(
                    new \Exception("Missing amount in interval definition '{$diff}'"),
                    'interval'
                );
            }

            return "{$func} {$incr} interval {$amount} {$types[$unit]}";
        }

        return $func;
    }

    // todo: beide sind noch nicht rund - muss noch getestet werden!!
    /*
     *
    public function interval(string $diff, string $func = 'NOW()'): string
    {
        $types = [
            's' => 'second',
            'm' => 'minute',
            'h' => 'hour',
            'd' => 'day',
            'M' => 'month',
            'Y' => 'year',
        ];

        // Fallback: Nur ein Segment vorhanden (alte Logik)
        if (preg_match('/^([+-]?)\s*(\d+)\s*([a-zA-Z])$/', $diff, $m)) {
            [$raw, $sign, $amount, $unit] = $m;

            if (!isset($types[$unit])) {
                throw new \Exception("Invalid interval unit '{$unit}' in '{$diff}'");
            }

            return "{$func} " . ($sign ?: '+') . " interval {$amount} {$types[$unit]}";
        }

        // Neue Logik: Mehrteilige Angabe wie '1Y2M3d'
        preg_match_all('/([+-]?)(\d+)([smhdMY])/', $diff, $matches, PREG_SET_ORDER);

        if (empty($matches)) {
            throw new \Exception("Invalid interval format '{$diff}'");
        }

        $parts = [];

        foreach ($matches as [$full, $sign, $amount, $unit]) {
            if (!isset($types[$unit])) {
                throw new \Exception("Invalid interval unit '{$unit}' in '{$diff}'");
            }

            $parts[] = ($sign === '-' ? '-' : '+') . " interval {$amount} {$types[$unit]}";
        }

        return $func . ' ' . implode(' ', $parts);
    }
    */

    /**
     * Generates a SQL expression to toggle a boolean (e.g. `isActive = !isActive`)
     *
     * Used for inverting boolean values directly in UPDATE statements.
     * If no column is provided, the field being updated is assumed.
     *
     * Example:
     *   $db->update('users', ['isActive' => $db->not()]);
     *   → Generates: `isActive = !isActive`
     *
     * @param string|null $column Optional column name to negate. If null, uses field name in context.
     * @return array<string, string> SQL-compatible negation expression
     */
    public function not(?string $column = null): array
    {
        return ['[N]' => $column ?? ''];
    }

    /**
     * Generates a special insert/update expression for use in SQL statements
     * (e.g., for fields like "created_at" or "updated_at").
     *
     * Example:
     *   $this->now('-1d') → ['[F]' => ['NOW() - interval 1 day']]
     *
     * The result can be passed directly into insert/update data arrays:
     *   $db->insert('table', ['created' => $this->now()]);
     *
     * @param string|null $diff Optional interval like "-1d", "2h", etc.
     * @param string $func Base function or column to apply interval to (default: NOW())
     * @return array<string, array<string>> SQL-compatible functional expression
     * @throws \Exception
     */
    public function now(?string $diff = null, string $func = 'NOW()'): array
    {
        return ['[F]' => [$this->interval($diff ?? '', $func)]];
    }

    // Query Options

    /**
     * Locks one or more tables using the configured lock method (READ or WRITE).
     *
     * The lock mode is controlled by $this->_tableLockMethod, which defaults to WRITE.
     * To unlock previously locked tables, use {@see unlock()}.
     *
     * @param string|string[] $table Table name or array of table names to lock.
     * @return bool True on success, false on failure.
     *
     * @throws \Throwable If validation or query execution fails.
     */
    public function lock(string|array $table): bool
    {
        if ($this->isSubQuery) {
            $this->reset(true);
            throw $this->handleException(
                new \LogicException("Cannot use lock() inside a subquery instance."),
                'lock'
            );
        }

        if (!empty($this->_tableLocks[$this->defConnectionName])) {
            $this->reset(true);
            throw $this->handleException(
                new \LogicException("Cannot call lock() multiple times per session. Use unlock() first."),
                'lock'
            );
        }

        $prefix = $this->getPrefix();
        $this->_query = "LOCK TABLES ";
        $this->_tableLocks[$this->defConnectionName] = [];

        $parts = [];

        if (is_array($table)) {
            foreach ($table as $value) {
                if (!is_string($value)) {
                    continue;
                }

                try {
                    $validated = $this->_secureValidateTable($value);
                    $parts[] = $prefix . $validated . ' ' . $this->_tableLockMethod[$this->defConnectionName];
                    $this->_tableLocks[$this->defConnectionName][] = $validated;
                } catch (\Throwable $e) {
                    $this->_unlockOnFailure();
                    $this->_beforeReset();
                    return $this->handleException($e, "lock [table={$value}]");
                }
            }
        } else {
            try {
                $validated = $this->_secureValidateTable($table);
                $this->_query .= $prefix . $validated . ' ' . $this->_tableLockMethod[$this->defConnectionName];
                $this->_tableLocks[$this->defConnectionName][] = $validated;
            } catch (\Throwable $e) {
                $this->_unlockOnFailure();
                $this->_beforeReset();
                return $this->handleException($e, "lock [table={$table}]");
            }
        }

        if (!empty($parts)) {
            $this->_query .= implode(', ', $parts);
        }

        try {
            $result = $this->queryUnprepared($this->_query);
            $this->reset();
            return (bool)$result;
        } catch (\Throwable $e) {
            $this->_unlockOnFailure();
            $this->reset(true);
            return $this->handleException($e, 'lock');
        }
    }

    protected function _unlockOnFailure(): void
    {
        if (!empty($this->_tableLocks[$this->defConnectionName])) {
            try {
                $this->queryUnprepared('UNLOCK TABLES');
            } catch (\Throwable $e) {
                $this->logException($e, 'unlock failure');
            } finally {
                $this->_tableLockMethod[$this->defConnectionName] = 'READ';
                $this->_tableLocks[$this->defConnectionName] = [];
                $this->reset(true);
            }
        }
    }
    /**
     * Sets the current table lock method.
     *
     * @param string $method The table lock method. Allowed values: 'READ' or 'WRITE'.
     *
     * @throws \InvalidArgumentException|\Throwable If an invalid lock method is provided.
     * @return static Returns the current instance for method chaining.
     */
    public function setLockMethod(string $method): self
    {
        $method = strtoupper($method);
        if ($method === 'READ' || $method === 'WRITE') {
            $this->_tableLockMethod[$this->defConnectionName] = $method;
        } else {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid lock type: must be either 'READ' or 'WRITE'."),
                'lock'
            );
        }

        return $this;
    }

    /**
     * Sets one or multiple SQL query options (e.g. DISTINCT, SQL_CALC_FOUND_ROWS).
     * Allows chaining: $db->setQueryOption(['DISTINCT', 'SQL_CALC_FOUND_ROWS']);
     *
     * @param string|array $options One or multiple query option names.
     * @throws \InvalidArgumentException|\Throwable if an unknown option is provided.
     * @return self
     */
    public function setQueryOption(string|array $options): self
    {
        $allowedOptions = [
            'ALL', 'DISTINCT', 'DISTINCTROW', 'HIGH_PRIORITY', 'STRAIGHT_JOIN', 'SQL_SMALL_RESULT',
            'SQL_BIG_RESULT', 'SQL_BUFFER_RESULT', 'SQL_CACHE', 'SQL_NO_CACHE', 'SQL_CALC_FOUND_ROWS',
            'LOW_PRIORITY', 'IGNORE', 'QUICK', 'MYSQLI_NESTJOIN', 'FOR UPDATE', 'LOCK IN SHARE MODE'
        ];

        foreach ((array) $options as $option) {
            $option = strtoupper($option);

            if (!in_array($option, $allowedOptions, true)) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Invalid SQL query option: {$option}"),
                    'setQueryOption'
                );
            }

            // Internal flags for certain keywords
            if ($option === 'MYSQLI_NESTJOIN') {
                $this->_nestJoin = true;
            } elseif ($option === 'FOR UPDATE') {
                $this->_forUpdate = true;
            } elseif ($option === 'LOCK IN SHARE MODE') {
                $this->_lockInShareMode = true;
            } else {
                if (!in_array($option, $this->_queryOptions, true)) {
                    $this->_queryOptions[] = $option;
                }
            }
        }

        return $this;
    }

    /**
     * Unlocks all tables in the current database connection.
     * Also commits any pending transactions.
     *
     * @return static Fluent interface for chaining.
     * @throws \Exception If unlocking fails.
     */
    public function unlock(): self
    {
        if ($this->isSubQuery) {
            $this->_unlockOnFailure();
            $this->reset(true);
            throw $this->handleException(
                new \LogicException("Cannot unlock() inside a subquery instance."),
                'unlock'
            );
        }

        $this->_query = "UNLOCK TABLES";

        try {
            $result = $this->queryUnprepared($this->_query);
            $this->_tableLockMethod[$this->defConnectionName] = 'READ';
            $this->_tableLocks[$this->defConnectionName] = [];
            $this->reset();

            if ($result) {
                return $this;
            }
            throw new \Exception("Unlocking tables failed.");
        } catch (\Exception $e) {
            $this->_unlockOnFailure();
            $this->reset(true);
            throw $this->handleException($e, 'unlock');
        }
    }

    // SubQuery

    /**
     * Returns the query structure of this object if it's a subquery.
     * Used for embedding subqueries in larger statements.
     *
     * @return array|null Array with keys: query, params, alias – or null if not a subquery.
     */
    public function getSubQuery(): ?array
    {
        if (!$this->isSubQuery) {
            return null;
        }

        // Optional: Dummy-Eintrag entfernen, falls vorhanden
        if (isset($this->_bindParams[0]) && $this->_bindParams[0]['value'] === null) {
            array_shift($this->_bindParams);
        }

        $flatParams = [];
        foreach ($this->_bindParams as $param) {
            $flatParams[] = is_array($param) && array_key_exists('value', $param)
                ? $param['value']
                : $param;
        }

        $query = $this->_query;
        $this->reset();

        return [
            'query'  => $query,
            'params' => $flatParams,
            'alias'  => $this->connectionsSettings[$this->defConnectionName]['host'] ?? null,
        ];
    }

    /**
     * Creates a new instance of the class in subquery mode.
     *
     * This method allows embedding a subquery within a parent query.
     * The `$subQueryAlias` is used as a placeholder for aliasing purposes
     * when the subquery is inserted into a larger query context.
     *
     * Example:
     *   $sub = DB::subQuery('alias');
     *   $sub->get('users');
     *   $db->join($sub, 'u.id = users.id', 'LEFT');
     *
     * @param string $subQueryAlias Alias name for the subquery (optional)
     * @return self Instance configured as subquery
     */
    public function subQuery(string $subQueryAlias = ''): self
    {
        return new self([
            'host'       => $subQueryAlias,
            'isSubQuery' => true,
            'instanz'    => $this->defConnectionName,
        ]);
    }

    // Transaction

    /**
     * Begins a new database transaction using PDO.
     *
     * @throws \Exception|\Throwable if transaction start fails
     */
    public function startTransaction(): void
    {
        $pdo = $this->connect($this->defConnectionName);

        if ($pdo->inTransaction()) {
            $e = new \RuntimeException("Transaction already active");
            $this->handleException($e, 'startTransaction');
        }

        if (!$pdo->beginTransaction()) {
            throw new \RuntimeException("Unable to begin transaction");
        }

        $this->_transaction_in_progress[$this->defConnectionName] = true;
    }

    /**
     * Commits the current transaction.
     *
     * @throws \Exception if no transaction is active or commit fails.
     * @throws \Throwable
     */
    public function commit(): void
    {
        $pdo = $this->connect($this->defConnectionName);

        if (!$pdo->inTransaction()) {
            $this->handleException(
                new \Exception("No active transaction to commit"),
                'commit'
            );
        }

        if (!$pdo->commit()) {
            $this->handleException(
                new \Exception("Transaction commit failed"),
                'commit'
            );
        }

        $this->_transaction_in_progress[$this->defConnectionName] = false;
    }

    /**
     * Rolls back the current transaction.
     *
     * @throws \Exception if no transaction is active or rollback fails.
     * @throws \Throwable
     */
    public function rollback(): void
    {
        $pdo = $this->connect($this->defConnectionName);

        if (!$pdo->inTransaction()) {
            $this->handleException(
                new \Exception("No active transaction to rollback."),
                'rollback'
            );
        }

        try {
            if (!$pdo->rollBack()) {
                $this->handleException(
                    new \Exception("Transaction rollback failed."),
                    'rollback'
                );
            }

            $this->_transaction_in_progress[$this->defConnectionName] = false;
        } catch (\Throwable $e) {
            $this->_transaction_in_progress[$this->defConnectionName] = false;
            $this->reset(true);
            $this->handleException($e, 'rollback');
        }
    }

    /**
     * Shutdown handler to rollback uncommitted operations in order to keep
     * atomic operations sane. Only called when a transaction was left open.
     */
    protected function _transaction_status_check(): void
    {
        try {
            if ($this->_transaction_in_progress[$this->defConnectionName]) {
                $this->rollback();
            }
        } catch (\Throwable $e) {
            error_log("Shutdown rollback failed: " . $e->getMessage());
        }
    }

    // Clauses & Helpers

    // WHERE Clauses
    /**
     * Adds an AND WHERE condition to the query.
     *
     * @param string $column Column name
     * @param mixed $value Value to compare
     * @param string $operator Comparison operator (default '=')
     * @return self
     * @throws \Throwable
     */
    public function where(string $column, mixed $value = null, string $operator = '='):self
    {
        return $this->secureWhere($column, $value, $operator, 'AND');
    }

    /**
     * Adds a strict boolean condition to the WHERE clause using AND logic.
     *
     * Only accepts the values true, false, 1, or 0 (strict comparison).
     * Throws an InvalidArgumentException for any other value.
     * Internally converts the value to 1 (true) or 0 (false) for the query.
     *
     * Example:
     * ```php
     * $db->whereBool('active', true);
     * ```
     *
     * @param string $column   The column name to filter.
     * @param mixed  $value    Must be one of: true, false, 1, or 0 (strict).
     * @param string $operator Optional SQL operator (default '=').
     *
     * @return self          Fluent interface for chaining.
     * @throws \InvalidArgumentException|\Throwable If the value is invalid.
     */
    public function whereBool(string $column, mixed $value, string $operator = '='): self
    {
        if (!in_array($value, [true, false, 1, 0], true)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid boolean value for column '{$column}'"),
                "whereBool [column={$column} value=" . var_export($value, true) . "]"
            );
        }

        return $this->secureWhere($column, (bool)$value ? 1 : 0, $operator, 'AND');
    }

    /**
     * Adds a WHERE condition for a single date value (e.g. =, <, >).
     * Accepts Y-m-d, Y-m-d H:i:s, or 10-digit timestamps.
     *
     * @param string $column The column name to filter
     * @param mixed $value A date string or timestamp
     * @param string $operator Optional SQL operator (default '=')
     * @return self
     * @throws \Throwable
     */
    public function whereDate(string $column, mixed $value, string $operator = '='): self
    {
        $date = $this->_secureValidateAndFormatDate($value);
        return $this->secureWhere($column, $date, $operator, 'AND');
    }

    /**
     * Adds a WHERE condition to filter between two dates (inclusive).
     *
     * @param string $column The column name to filter
     * @param mixed $fromDate Start date (string or timestamp)
     * @param mixed $toDate End date (string or timestamp)
     * @return self
     * @throws \Throwable
     */
    public function whereDateBetween(string $column, mixed $fromDate, mixed $toDate): self
    {
        $from = $this->_secureValidateAndFormatDate($fromDate);
        $to   = $this->_secureValidateAndFormatDate($toDate);

        return $this->secureWhere($column, [$from, $to], 'BETWEEN', 'AND');
    }

    /**
     * Adds a WHERE condition to exclude a date range (NOT BETWEEN).
     *
     * @param string $column The column name to filter
     * @param mixed $fromDate Start date (string or timestamp)
     * @param mixed $toDate End date (string or timestamp)
     * @return self
     * @throws \Throwable
     */
    public function whereDateNotBetween(string $column, mixed $fromDate, mixed $toDate): self
    {
        $from = $this->_secureValidateAndFormatDate($fromDate);
        $to   = $this->_secureValidateAndFormatDate($toDate);

        return $this->secureWhere($column, [$from, $to], 'NOT BETWEEN', 'AND');
    }

    /**
     * Adds a WHERE condition with strict float validation.
     *
     * This method ensures that the provided value is a valid float, integer, or numeric string.
     * Invalid or non-numeric values will trigger an InvalidArgumentException.
     *
     * Example: $db->whereFloat('price', 19.99);
     *
     * @param string $column   The column name to compare.
     * @param mixed  $value    The value to compare. Must be float-compatible.
     * @param string $operator Optional comparison operator (default '=').
     *
     * @return self
     *
     * @throws \InvalidArgumentException|\Throwable
     */
    public function whereFloat(string $column, mixed $value, string $operator = '='):self
    {
        $tmp = (float)$value;

        if ((string)$tmp !== (string)$value && !is_float($value) && !is_int($value)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid float value for column '{$column}'."),
                "whereFloat [column={$column} value=" . var_export($value, true) . "]"
            );
        }

        return $this->secureWhere($column, (float)$value, $operator, 'AND');
    }

    /**
     * Adds a WHERE condition using SQL functions as columns (e.g., DATE(created_at)).
     *
     * Only allows safe, predefined SQL functions to prevent SQL injection.
     *
     * @param string $functionColumn A valid function-based column expression like "DATE(created_at)".
     * @param mixed  $value          The comparison value (e.g., '2024-01-01').
     * @param string $operator       SQL comparison operator (default '=').
     *
     * @return self                Fluent interface for chaining.
     * @throws \InvalidArgumentException|\Throwable If the function expression is unsafe.
     */
    public function whereFunc(string $functionColumn, mixed $value, string $operator = '='):self
    {
        if (!$this->_secureIsSafeColumn($functionColumn)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Unsafe function expression in WHERE: {$functionColumn}"),
                "whereFunc [expression={$functionColumn}]"
            );
        }

        return $this->secureWhere($functionColumn, $value, $operator, 'AND');
    }

    /**
     * Adds an AND WHERE IN condition to the query builder.
     *
     * This method strictly validates the column name to prevent SQL injection,
     * accepting only plain alphanumeric names, optionally with dots (e.g., 'id', 'table.column').
     * It also ensures that the provided array of values is not empty.
     *
     * Example usage:
     * `$db->whereIn('user_id', [1, 2, 3]);`
     * `$db->whereIn('category', ['electronics', 'books']);`
     *
     * @param string $column The column name to check against.
     * @param array  $values A non-empty array of scalar values to be included in the IN clause.
     * @return self
     * @throws \InvalidArgumentException|\Throwable If the column name is invalid or the values array is empty or contains non-scalar values.
     */
    public function whereIn(string $column, array $values): self
    {
        if (empty($values)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("whereIn() expects a non-empty array."),
                "whereIn"
            );
        }

        return $this->secureWhere($column, $values, 'IN', 'AND');
    }

    /**
     * Adds an AND WHERE condition expecting an integer value.
     *
     * Validates that the given value is a pure integer or a string representing an integer,
     * otherwise throws an InvalidArgumentException.
     *
     * @param string $column   Column name to filter on.
     * @param mixed  $value    The value to compare; must be integer or numeric string.
     * @param string $operator Comparison operator (default '=').
     * @return self          Fluent interface for chaining.
     * @throws \InvalidArgumentException|\Throwable If the value is not a valid integer.
     */
    public function whereInt(string $column, mixed $value, string $operator = '='):self
    {
        $tmp = (int)$value;

        if ((string)$tmp !== (string)$value && !is_int($value)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid integer value for column '{$column}'."),
                "whereInt"
            );
        }

        return $this->secureWhere($column, $tmp, $operator, 'AND');
    }

    /**
     * @throws \Throwable
     */
    public function whereIntIn(string $column, array $values): self
    {

        if (empty($values)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Empty array passed to whereIntIn()"),
                'whereIntIn'
            );
        }

        foreach ($values as $val) {
            if (!is_int($val) && !(is_string($val) && ctype_digit($val))) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Non-integer value detected in whereIntIn(): " . var_export($val, true)),
                    'whereIntIn'
                );
            }
        }

        $ints = array_map('intval', $values);
        return $this->secureWhere($column, $ints, 'IN');
    }

    /**
     * Adds an AND WHERE EQUAL condition to the query.
     *
     * This is a semantic helper for simple equality checks, using the '=' operator internally.
     *
     * Example:
     * ```php
     * $db->whereIs('status', 'active');
     * // Generates: WHERE `status` = ?
     * ```
     *
     * Example:
     * ```php
     * $db->whereIs('id', 123);
     * // Generates: WHERE `id` = ?
     * ```
     *
     * @param string $field The column name to check. Must be a safe alphanumeric name, optionally with dots.
     * @param mixed $value The value to compare against.
     * @return self       Fluent interface for chaining.
     * @throws \InvalidArgumentException If the column name is invalid.
     * @throws \Throwable
     */
    public function whereIs(string $field, mixed $value): self
    {
        return $this->secureWhere($field, $value, '=', 'AND');
    }

    /**
     * Adds an AND WHERE NOT EQUAL condition to the query.
     *
     * This is a semantic helper for simple inequality checks, using the '!=' operator internally.
     *
     * Example:
     * ```php
     * $db->whereIsNot('role', 'admin');
     * // Generates: WHERE `role` != ?
     * ```
     *
     * @param string $field The column name to check. Must be a safe alphanumeric name, optionally with dots.
     * @param mixed $value The value to compare against.
     * @return self       Fluent interface for chaining.
     * @throws \InvalidArgumentException If the column name is invalid.
     * @throws \Throwable
     */
    public function whereIsNot(string $field, mixed $value): self
    {
        return $this->secureWhere($field, $value, '!=', 'AND');
    }

    /**
     * Adds an AND WHERE IS NOT NULL condition to the query.
     *
     * Checks if the specified column's value is NOT NULL.
     *
     * Example:
     * ```php
     * $db->whereIsNotNull('email');
     * // Generates: WHERE `email` IS NOT NULL
     * ```
     *
     * @param string $field The column name to check. Must be a safe alphanumeric name, optionally with dots.
     * @return self       Fluent interface for chaining.
     * @throws \InvalidArgumentException If the column name is invalid.
     * @throws \Throwable
     */
    public function whereIsNotNull(string $field): self
    {
        return $this->secureWhere($field, null, 'IS NOT', 'AND');
    }

    /**
     * Adds an AND WHERE IS NULL condition to the query.
     *
     * Checks if the specified column's value is NULL.
     *
     * Example:
     * ```php
     * $db->whereIsNull('deleted_at');
     * // Generates: WHERE `deleted_at` IS NULL
     * ```
     *
     * @param string $field The column name to check. Must be a safe alphanumeric name, optionally with dots.
     * @return self       Fluent interface for chaining.
     * @throws \InvalidArgumentException If the column name is invalid.
     * @throws \Throwable
     */
    public function whereIsNull(string $field): self
    {
        return $this->secureWhere($field, null, 'IS', 'AND');
    }

    /**
     * Adds an AND WHERE NOT IN condition to the query builder.
     *
     * Strictly validates the column name to prevent SQL injection,
     * accepting only plain alphanumeric names, optionally with dots (e.g., 'id', 'table.column').
     * Ensures that the provided array of values is not empty.
     *
     * Example usage:
     * ```php
     * $db->whereNotIn('user_id', [4, 5, 6]);
     * $db->whereNotIn('status', ['archived', 'deleted']);
     * ```
     *
     * @param string $field  The column name to check against.
     * @param array  $values Non-empty array of scalar values to exclude.
     *
     * @return self        Fluent interface for chaining.
     * @throws \InvalidArgumentException|\Throwable If the values array is empty or contains non-scalar values.
     */
    public function whereNotIn(string $field, array $values): self
    {
        if (empty($values)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("whereNotIn() expects a non-empty array."),
                'whereNotIn'
            );
        }

        return $this->secureWhere($field, $values, 'NOT IN', 'AND');
    }

    /**
     * Adds a WHERE condition with safe string handling.
     *
     * Ensures the value is a scalar and safely castable to string.
     * Accepts strings of any length and delegates binding to PDO.
     *
     * Example:
     * ```php
     * $db->whereString('username', 'admin');
     * ```
     *
     * @param string $column   The column name to compare.
     * @param mixed  $value    The value to compare; will be cast to string.
     * @param string $operator Optional comparison operator (default '=').
     *
     * @return self          Fluent interface for chaining.
     * @throws \InvalidArgumentException|\Throwable If a non-scalar value is given.
     */
    public function whereString(string $column, mixed $value, string $operator = '='):self
    {
        if (!is_scalar($value)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Non-scalar value given for column '{$column}'."),
                "secureWhere [column={$column}]"
            );
        }

        return $this->secureWhere($column, (string)$value, $operator, 'AND');
    }

    /**
     * Adds a WHERE condition for timestamp-based columns.
     *
     * This is an alias for `whereInt()`, enforcing strict integer validation.
     * Accepts integer Unix timestamps or numeric strings.
     *
     * @param string $column Column name (e.g., 'created_at').
     * @param int|string $value Unix timestamp or numeric string (e.g., 1720000000).
     * @param string $operator SQL comparison operator (default '=').
     *
     * @return self              Fluent interface for chaining.
     * @throws \Throwable
     */
    public function whereTimestamp(string $column, int|string $value, string $operator = '='):self
    {
        return $this->whereInt($column, $value, $operator);
    }

    // orWHERE Clauses

    /**
     * Adds an OR WHERE condition to the query.
     *
     * @param string $column Column name
     * @param mixed $value Value to compare
     * @param string $operator Comparison operator (default '=')
     * @return self
     * @throws \Throwable
     */
    public function orWhere(string $column, mixed $value = null, string $operator = '='):self
    {
        return $this->secureWhere($column, $value, $operator, 'OR');
    }

    /**
     * Adds a strict boolean condition to the WHERE clause using OR logic.
     *
     * Only accepts the values true, false, 1, or 0 (strict comparison).
     * Throws an InvalidArgumentException for any other value.
     * Internally converts the value to 1 (true) or 0 (false) for the query.
     *
     * Example:
     * ```php
     * $db->orWhereBool('active', false);
     * ```
     *
     * @param string $column   The column name to filter.
     * @param mixed  $value    Must be one of: true, false, 1, or 0 (strict).
     * @param string $operator Optional SQL operator (default '=').
     *
     * @return self          Fluent interface for chaining.
     * @throws \InvalidArgumentException|\Throwable If the value is invalid.
     */
    public function orWhereBool(string $column, mixed $value, string $operator = '='): self
    {
        if (!in_array($value, [true, false, 1, 0], true)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid boolean value for column '{$column}'."),
                "whereBool [column={$column}]"
            );
        }

        return $this->secureWhere($column, (bool)$value ? 1 : 0, $operator, 'OR');
    }

    /**
     * Adds an OR WHERE condition for a single date value.
     *
     * @param string $column The column name to filter
     * @param mixed $value A date string or timestamp
     * @param string $operator Optional SQL operator (default '=')
     * @return self
     * @throws \Throwable
     */
    public function orWhereDate(string $column, mixed $value, string $operator = '='): self
    {
        $date = $this->_secureValidateAndFormatDate($value);
        return $this->secureWhere($column, $date, $operator, 'OR');
    }

    /**
     * Adds an OR WHERE condition to filter between two dates.
     *
     * @param string $column The column name to filter
     * @param mixed $fromDate Start date (string or timestamp)
     * @param mixed $toDate End date (string or timestamp)
     * @return self
     * @throws \Throwable
     */
    public function orWhereDateBetween(string $column, mixed $fromDate, mixed $toDate): self
    {
        $from = $this->_secureValidateAndFormatDate($fromDate);
        $to   = $this->_secureValidateAndFormatDate($toDate);

        return $this->secureWhere($column, [$from, $to], 'BETWEEN', 'OR');
    }

    /**
     * Adds an OR WHERE condition to exclude a date range (NOT BETWEEN).
     *
     * @param string $column The column name to filter
     * @param mixed $fromDate Start date (string or timestamp)
     * @param mixed $toDate End date (string or timestamp)
     * @return self
     * @throws \Throwable
     */
    public function orWhereDateNotBetween(string $column, mixed $fromDate, mixed $toDate): self
    {
        $from = $this->_secureValidateAndFormatDate($fromDate);
        $to   = $this->_secureValidateAndFormatDate($toDate);

        return $this->secureWhere($column, [$from, $to], 'NOT BETWEEN', 'OR');
    }

    /**
     * Adds an OR WHERE condition with strict float validation.
     *
     * Ensures the value is a valid float, integer, or numeric string.
     * Throws InvalidArgumentException if invalid.
     *
     * Example:
     * ```php
     * $db->orWhereFloat('amount', '120.50');
     * ```
     *
     * @param string $column   The column name to compare.
     * @param mixed  $value    The value to compare; must be float-compatible.
     * @param string $operator Optional comparison operator (default '=').
     *
     * @return self          Fluent interface for chaining.
     * @throws \InvalidArgumentException|\Throwable
     */
    public function orWhereFloat(string $column, mixed $value, string $operator = '='):self
    {
        $tmp = (float)$value;

        if ((string)$tmp !== (string)$value && !is_float($value) && !is_int($value)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid float value for column '{$column}'."),
                "whereFloat [column={$column}]"
            );
        }

        return $this->secureWhere($column, $tmp, $operator, 'OR');
    }

    /**
     * OR WHERE condition using SQL functions as columns (e.g., DATE(created_at)).
     * Only allows safe, predefined SQL functions to prevent injection.
     *
     * @param string $functionColumn A valid function-based column like "DATE(created_at)"
     * @param mixed $value Comparison value (e.g. '2024-01-01')
     * @param string $operator SQL comparison operator (default '=')
     * @return self
     * @throws \Throwable
     */
    public function orWhereFunc(string $functionColumn, mixed $value, string $operator = '='):self
    {
        if (!$this->_secureIsSafeColumn($functionColumn)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Unsafe function expression in WHERE: {$functionColumn}"),
                "secureWhere [functionColumn={$functionColumn}]"
            );
        }

        return $this->secureWhere($functionColumn, $value, $operator, 'OR');
    }

    /**
     * Adds an OR WHERE IN condition with an array of values.
     *
     * @param string $field The column name to filter.
     * @param array $values Non-empty array of values for the IN clause.
     *
     * @return self        Fluent interface for chaining.
     * @throws \InvalidArgumentException If the array is empty.
     * @throws \Throwable
     */
    public function orWhereIn(string $field, array $values): self
    {
        if (empty($values)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("orWhereIn() expects a non-empty array."),
                "orWhereIn"
            );
        }

        return $this->secureWhere($field, $values, 'IN', 'OR');

    }

    /**
     * Adds an OR WHERE condition expecting a clean integer value.
     *
     * Validates that the value is a pure integer or numeric string,
     * rejects malformed or dangerous inputs (e.g. '1); DROP ...').
     *
     * @param string $column Column name to filter on.
     * @param mixed $value Value to compare; must be an integer or integer string.
     * @param string $operator Comparison operator (default '=').
     * @return self          Fluent interface for chaining.
     * @throws \InvalidArgumentException If the value is not a valid integer.
     * @throws \Throwable
     */
    public function orWhereInt(string $column, mixed $value, string $operator = '='):self
    {
        $tmp = (int)$value;

        if ((string)$tmp !== (string)$value && !is_int($value)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid integer value for column '{$column}'."),
                'whereInt'
            );
        }

        return $this->secureWhere($column, $tmp, $operator, 'OR');
    }

    /**
     * @throws \Throwable
     */
    public function orWhereIntIn(string $column, array $values): self
    {
        if (empty($values)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Empty array passed to orWhereIntIn()"),
                'orWhereIntIn'
            );

        }

        foreach ($values as $val) {
            if (!is_int($val) && !(is_string($val) && ctype_digit($val))) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Non-integer value detected in orWhereIntIn(): " . var_export($val, true)),
                    'orWhereIntIn'
                );
            }
        }

        $ints = array_map('intval', $values);
        return $this->secureWhere($column, $ints, 'IN', 'OR');
    }

    /**
     * Adds an OR WHERE EQUAL condition to the query.
     *
     * @param string $field The column name to check.
     * @param mixed $value The value to compare against.
     * @return self
     * @throws \InvalidArgumentException If the column name is invalid.
     * @throws \Throwable
     */
    public function orWhereIs(string $field, mixed $value): self
    {
        return $this->secureWhere($field, $value, '=', 'OR');
    }

    /**
     * Adds an OR WHERE NOT EQUAL condition to the query.
     *
     * @param string $field The column name to check.
     * @param mixed $value The value to compare against.
     * @return self
     * @throws \InvalidArgumentException If the column name is invalid.
     * @throws \Throwable
     */
    public function orWhereIsNot(string $field, mixed $value): self
    {
        return $this->secureWhere($field, $value, '!=', 'OR');
    }

    /**
     * Adds an OR WHERE IS NOT NULL condition to the query.
     *
     * @param string $field The column name to check.
     * @return self
     * @throws \InvalidArgumentException If the column name is invalid.
     * @throws \Throwable
     */
    public function orWhereIsNotNull(string $field): self
    {
        return $this->secureWhere($field, null, 'IS NOT', 'OR');
    }

    /**
     * Adds an OR WHERE IS NULL condition to the query.
     *
     * @param string $field The column name to check.
     * @return self
     * @throws \InvalidArgumentException If the column name is invalid.
     * @throws \Throwable
     */
    public function orWhereIsNull(string $field): self
    {
        return $this->secureWhere($field, null, 'IS', 'OR');
    }

    /**
     * Adds an OR WHERE NOT IN condition to the query builder.
     *
     * @param string $field The column name to check against.
     * @param array $values Non-empty array of scalar values to exclude.
     *
     * @return self        Fluent interface for chaining.
     * @throws \InvalidArgumentException If the values array is empty.
     * @throws \Throwable
     */
    public function orWhereNotIn(string $field, array $values): self
    {
        if (empty($values)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("orWhereNotIn() expects a non-empty array."),
                'orWhereNotIn'
            );
        }

        return $this->secureWhere($field, $values, "NOT IN", 'OR');
    }

    /**
     * Adds an OR WHERE condition with safe string handling.
     *
     * Ensures the value is a scalar and safely castable to string.
     * Accepts strings of any length and delegates binding to PDO.
     *
     * Example:
     * ```php
     * $db->orWhereString('description', 'example text');
     * ```
     *
     * @param string $column The column name to compare.
     * @param mixed $value The value to compare; will be cast to string.
     * @param string $operator Optional comparison operator (default '=').
     *
     * @return self          Fluent interface for chaining.
     * @throws \InvalidArgumentException If a non-scalar value is given.
     * @throws \Throwable
     */
    public function orWhereString(string $column, mixed $value, string $operator = '='):self
    {
        if (!is_scalar($value)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Non-scalar value given for column '{$column}'."),
                'where'
            );

        }

        return $this->secureWhere($column, (string)$value, $operator, 'OR');
    }

    /**
     * Adds an OR WHERE condition for timestamp-based columns.
     *
     * This is an alias for `orWhereInt()`, enforcing strict integer validation.
     * Accepts integer Unix timestamps or numeric strings.
     *
     * @param string $column Column name (e.g., 'created_at').
     * @param int|string $value Unix timestamp or numeric string (e.g., 1720000000).
     * @param string $operator SQL comparison operator (default '=').
     *
     * @return self              Fluent interface for chaining.
     * @throws \Throwable
     */
    public function orWhereTimestamp(string $column, int|string $value, string $operator = '='):self
    {
        return $this->orWhereInt($column, $value, $operator);
    }

    /**
     * Adds a WHERE condition to the query builder.
     *
     * Performs column and operator validation, handles NULL logic,
     * ensures scalar or array values only, and appends the condition
     * to the internal _where stack.
     *
     * @param string $column   The column name (validated)
     * @param mixed  $value    The value or operator if shorthand
     * @param string $operator Comparison operator (defaults to '=')
     * @param string $cond     Logical condition: AND / OR
     * @return self
     * @throws \InvalidArgumentException|\Throwable
     */
    protected function secureWhere(string $column, mixed $value = null, string $operator = '=', string $cond = 'AND'): self
    {
        if (count($this->_where) === 0) {
            $cond = '';
        }

        if (preg_match('/^\s*([a-z_]+)\s*\(/i', $column, $m)) {
            $func = strtoupper($m[1]);
            $aggFuncs = ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT', 'GROUP_CONCAT'];
            if (in_array($func, $aggFuncs, true)) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Aggregate function '{$func}()' is not allowed in WHERE clause."),
                    "secureWhere [function={$func}]"
                );
            }
        }

        if (!$this->_secureIsSafeColumn($column)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Unsafe WHERE clause rejected: {$column}"),
                "secureWhere [column={$column}]"
            );
        }

        if (is_object($value)) {
            if (!method_exists($value, 'getSubQuery')) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Object in WHERE must implement getSubQuery()"),
                    "secureWhere [column={$column}]"
                );
            }

            $this->_where[] = [$cond, $column, $operator, $value];
            return $this;
        }

        $opUpper = strtoupper(trim($operator));

        // Platzhalter [F], [I], [N]
        if (
            is_array($value) && count($value) === 1 &&
            (isset($value['[F]']) || isset($value['[I]']) || isset($value['[N]']))
        ) {
            $validated = null;

            foreach ($value as $flag => $val) {
                switch ($flag) {
                    case '[F]':
                        if (!is_array($val) || count($val) !== 1) {
                            $this->reset(true);
                            throw $this->handleException(
                                new \InvalidArgumentException("Invalid [F] value: must be array with one function"),
                                "secureWhere [column={$column} flag={$flag}]"
                            );
                        }

                        $validated = [$flag => [$this->_secureValidateFunc($val[0])]];
                        break;

                    case '[I]':
                        if (!is_array($val)) {
                            $this->reset(true);
                            throw $this->handleException(
                                new \InvalidArgumentException("Invalid [I] value: must be array"),
                                "secureWhere [column={$column} flag={$flag}]"
                            );
                        }

                        foreach ($val as $identifier) {
                            if (!preg_match('/^[a-zA-Z0-9_\.]+$/', $identifier)) {
                                $this->reset(true);
                                throw $this->handleException(
                                    new \InvalidArgumentException("Invalid identifier in [I]: {$identifier}"),
                                    "secureWhere [column={$column} flag={$flag}]"
                                );
                            }
                        }

                        $validated = [$flag => $val];
                        break;

                    case '[N]':
                        if (!is_array($val)) {
                            $this->reset(true);
                            throw $this->handleException(
                                new \InvalidArgumentException("Invalid [N] value: must be array"),
                                "secureWhere [column={$column} flag={$flag}]"
                            );
                        }

                        foreach ($val as $num) {
                            if (!is_int($num)) {
                                $this->reset(true);
                                throw $this->handleException(
                                    new \InvalidArgumentException("Non-integer value in [N]: " . json_encode($num)),
                                    "secureWhere [column={$column} flag={$flag}]"
                                );
                            }
                        }

                        $validated = [$flag => $val];
                        break;

                    default:
                        $this->reset(true);
                        throw $this->handleException(
                            new \InvalidArgumentException("Unknown placeholder: {$flag}"),
                            "secureWhere [column={$column}]"
                        );
                }
            }

            $this->_where[] = [$cond, $column, $operator, $validated];
            return $this;
        }

        if (in_array($opUpper, ['IS', 'IS NOT'], true) && !is_null($value)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("IS / IS NOT only supports a real NULL value."),
                "secureWhere [column={$column}]"
            );
        }

        if (is_null($value) && in_array($opUpper, ['IS', 'IS NOT'], true)) {
            $this->_where[] = [$cond, $column, $opUpper, null];
            return $this;
        }

        if ($value === null && !in_array($opUpper, ['IS', 'IS NOT'], true)) {
            $value = $operator;
            $operator = '=';
            $opUpper = '=';
        }

        if (!$this->_secureIsSafeColumn($column) || !$this->_secureIsAllowedOperator($opUpper)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Unsafe WHERE clause rejected: {$column} {$operator}"),
                "secureWhere [column={$column} operator={$operator}]"
            );
        }

        // Frühzeitige Prüfung auf verdächtige skalare Werte
        if (is_scalar($value) && $this->_secureLooksSuspicious($value)) {

            if (PDOdb_HEURISTIC_WHERE_CHECK){
                $this->_where[] = [
                    '__DEFERRED__' => true,
                    'cond'     => $cond,
                    'column'   => $column,
                    'operator' => $operator,
                    'value'    => $value,
                ];
                return $this;
            } else {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Suspicious value in WHERE clause: {$column} = {$value}"),
                    "secureWhere [column={$column} value={$value}]"
                );
            }

        }

        // IN / NOT IN
        if (is_array($value) && in_array($opUpper, ['IN', 'NOT IN'], true)) {
            foreach ($value as $k => $val) {
                if (!is_scalar($val) && !is_null($val)) {
                    $this->reset(true);
                    throw $this->handleException(
                        new \InvalidArgumentException("Invalid value type in array for WHERE clause."),
                        "secureWhere [column={$column}]"
                    );
                }

                if ($this->_secureLooksSuspicious($k) || $this->_secureLooksSuspicious($val)) {
                    $this->reset(true);
                    throw $this->handleException(
                        new \InvalidArgumentException("Suspicious WHERE array value: " . json_encode([$k => $val])),
                        "secureWhere [column={$column}]"
                    );
                }
            }

            $this->_where[] = [$cond, $column, $opUpper, $value];
            return $this;
        }

        // BETWEEN / NOT BETWEEN
        if (is_array($value) && in_array($opUpper, ['BETWEEN', 'NOT BETWEEN'], true)) {
            if (count($value) !== 2) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("BETWEEN expects exactly 2 values."),
                    "secureWhere [column={$column}]"
                );
            }

            foreach ($value as $val) {
                if (!is_scalar($val)) {
                    $this->reset(true);
                    throw $this->handleException(
                        new \InvalidArgumentException("BETWEEN values must be scalar."),
                        "secureWhere [column={$column}]"
                    );
                }
            }

            $this->_where[] = [$cond, $column, $opUpper, $value];
            return $this;
        }

        if (is_array($value)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Operator '{$operator}' not allowed for array values."),
                "secureWhere [column={$column}]"
            );
        }

        if (!is_scalar($value)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid value type for WHERE clause."),
                "secureWhere [column={$column}]"
            );
        }

        $this->_where[] = [$cond, $column, $operator, $value];
        return $this;
    }

    // Having

    /**
     * Adds a HAVING condition to the SQL query, with optional operator and logical chaining (AND).
     *
     * Supports special values:
     * - 'DBNULL'    → IS NULL
     * - 'DBNOTNULL' → IS NOT NULL
     *
     * Example:
     * ```php
     * $db->having('SUM(score)', 10, '>')->orHaving('COUNT(id)', 'DBNOTNULL');
     * ```
     *
     * @param string $column Column or expression for the HAVING clause.
     * @param mixed $value Value to compare against, or special string like 'DBNULL'.
     * @param string $operator Comparison operator (default '=').
     *
     * @return self          Fluent interface for chaining.
     * @throws \Throwable
     */
    public function having(string $column, mixed $value = null, string $operator = '='):self
    {
        return $this->secureHaving($column, $value, $operator, 'AND');
    }

    /**
     * Adds a HAVING condition to the SQL query, with optional operator and logical chaining (OR).
     *
     * @param string $column Column or expression for the HAVING clause.
     * @param mixed $value Value to compare against, or special string like 'DBNULL'.
     * @param string $operator Comparison operator (default '=').
     *
     * @return self          Fluent interface for chaining.
     * @throws \Throwable
     */
    public function orHaving(string $column, mixed $value = null, string $operator = '='):self
    {
        return $this->secureHaving($column, $value, $operator, 'OR');
    }

    /**
     * Adds a HAVING condition to the internal HAVING stack with validation.
     *
     * Handles special values ('DBNULL', 'DBNOTNULL'), operator normalization,
     * and ensures safe columns and operators.
     *
     * @param string $column   Column or expression for HAVING clause.
     * @param mixed  $value    Value to compare against, or special values like 'DBNULL'.
     * @param string $operator SQL comparison operator (default '=').
     * @param string $cond     Logical condition to join with previous (AND/OR).
     *
     * @return self          Fluent interface for chaining.
     * @throws \InvalidArgumentException|\Throwable On invalid or unsafe input.
     */
    protected function secureHaving(string $column, mixed $value = null, string $operator = '=', string $cond = 'AND'): self
    {
        if (count($this->_pendingHaving) === 0) {
            $cond = '';
        }

        // Kurzform-Notation: ['>=' => 2]
        if (is_array($value) && count($value) === 1 && is_string(key($value))) {
            $operator = key($value);
            $value = current($value);
        }

        // Null-Alias-Notation
        if ($value === 'DBNULL') {
            $operator = 'IS';
            $value = null;
        } elseif ($value === 'DBNOTNULL') {
            $operator = 'IS NOT';
            $value = null;
        }

        // Nur IS / IS NOT dürfen mit NULL verwendet werden
        if ($value === null && !in_array(strtoupper($operator), ['IS', 'IS NOT'], true)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Unsafe HAVING clause: NULL requires IS or IS NOT operator."),
                'secureHaving'
            );
        }

        // Sonderfall: false als Value ist nicht erlaubt
        if ($value === false) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Unsafe HAVING clause: boolean FALSE not allowed."),
                'secureHaving'
            );
        }

        // Operator normalisieren
        $op = strtoupper($operator);

        // BETWEEN braucht exakt zwei Werte
        if (in_array($op, ['BETWEEN', 'NOT BETWEEN'], true)) {
            if (!is_array($value) || count($value) !== 2) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("HAVING operator '{$op}' requires an array with exactly two values."),
                    'secureHaving'
                );
            }
        }

        // IN braucht ein valides Array
        if (in_array($op, ['IN', 'NOT IN'], true)) {
            if (!is_array($value) || empty($value)) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("HAVING operator '{$op}' requires a non-empty array."),
                    'secureHaving'
                );
            }
        }

        // Spezial-Placeholder oder SubQuery validieren (wenn es einer ist)
        if (
            ($value instanceof self && $value->isSubQuery === true)
            || (is_array($value) && count($value) === 1 && in_array(key($value), ['[F]', '[I]', '[N]'], true))
        ) {
            try {
                $this->_secureValidateSpecialValue($value);
            } catch (\Throwable $e) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Invalid special value in HAVING clause: " . $e->getMessage(), 0, $e),
                    'secureHaving'
                );
            }
        }

        // Sicherheit: Column und Operator prüfen
        if (!$this->_secureIsSafeColumn($column) || !$this->_secureIsAllowedOperator($operator)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Unsafe HAVING clause rejected: {$column} {$operator}"),
                'secureHaving'
            );
        }

        // Column muss erlaubt sein: Aggregat, Alias oder GroupBy
        $isAlias      = in_array($column, $this->_selectAliases ?? [], true);
        $isGrouped    = $this->_isInGroupBy($column);
        $isAggregated = $this->_isAggregateFunction($column);

        if (!$isAlias && !$isGrouped && !$isAggregated) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid HAVING column '{$column}': must be an alias, aggregate or in GROUP BY"),
                'secureHaving'
            );

        }

        // Deferred speichern
        $this->_pendingHaving[] = [
            $cond,
            $column,
            $operator,
            $value
        ];

        return $this->_instance ?? $this;
    }

    // Joins
    /**
     * Adds a JOIN clause to the query.
     *
     * Supports simple joins and subqueries:
     * ```php
     * $db->join('users u', 'u.id = p.user_id', 'LEFT');
     * $db->join($db->subQuery('orders'), 'o.user_id = u.id', 'INNER');
     * ```
     *
     * @param string|self $joinTable     Table name or alias (subqueries should be handled as strings or objects with __toString).
     * @param string $joinCondition SQL ON condition, e.g. 'a.id = b.ref_id'.
     * @param string $joinType      Join type: 'LEFT', 'INNER', etc. (default: 'LEFT').
     *
     * @return self
     * @throws \InvalidArgumentException|\Throwable If the join type or table name is invalid.
     */
    public function join(string|self $joinTable, string $joinCondition, string $joinType = 'LEFT'): self
    {
        $allowedTypes = ['LEFT', 'RIGHT', 'OUTER', 'INNER', 'LEFT OUTER', 'RIGHT OUTER', 'NATURAL'];
        $joinType = strtoupper(trim($joinType));

        if ($joinType && !in_array($joinType, $allowedTypes)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Wrong JOIN type: {$joinType}"),
                'join'
            );
        }

        // Sicherheitscheck nur für Strings
        if (is_string($joinTable)) {
            if (str_starts_with(trim($joinTable), '(')) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Raw subqueries as string are not allowed. Use subQuery() object."),
                    'join'
                );
            }

            // TableName validieren
            try {
                $joinTable = $this->_secureValidateTable($joinTable);
            } catch (\InvalidArgumentException $e) {
                $this->reset(true);
                $this->logException($e, "join [table={$joinTable}]");
                throw $e;
            }
        }

        $this->_pendingJoins[] = [$joinType, $joinTable, $joinCondition];
        return $this;
    }

    /**
     * Adds a conditional ON-clause to a previously defined JOIN.
     *
     * This allows additional WHERE-style filters for joined tables.
     * These conditions are stored internally and applied when building the JOIN.
     *
     * Example:
     *   $db->join('user u', 'u.role_id = r.id', 'LEFT')
     *      ->joinWhere('user u', 'u.active', 1)
     *      ->joinWhere('user u', 'u.deleted', 0);
     *
     * @param string      $table      The joined table name (with optional alias, e.g. "user u").
     * @param string      $column     The column name of the joined table.
     * @param mixed       $value      The value to compare against (default is DBNULL).
     * @param string      $operator   SQL operator (=, >, <, IN, etc.), default is '='.
     * @param string      $concat     Logical operator (AND/OR), default is 'AND'.
     *
     * @return self
     */
    public function joinWhere(
        string $table,
        string $column,
        mixed $value = 'DBNULL',
        string $operator = '=',
        string $concat = 'AND'
    ): self {

        $table = $this->_secureValidateTable($table);

        $prefix = $this->getPrefix();
        $tableKey = $prefix . $table;

        if (!isset($this->_joinWheres[$tableKey])) {
            $this->_joinWheres[$tableKey] = [];
        }

        $this->_joinWheres[$tableKey][] = [$concat, $column, $operator, $value];
        return $this;
    }

    /**
     * Adds a conditional OR-clause to a previously defined JOIN.
     *
     * Internally calls joinWhere() with logical operator set to OR.
     *
     * Example:
     *   $db->join('user u', 'u.role_id = r.id', 'LEFT')
     *      ->joinOrWhere('user u', 'u.status', 'active');
     *
     * @param string $table
     * @param string $column
     * @param mixed $value
     * @param string $operator SQL operator (=, >, <, IN, etc.)
     *
     * @return self
     */
    public function joinOrWhere(
        string $table,
        string $column,
        mixed $value = 'DBNULL',
        string $operator = '='
    ): self {
        return $this->joinWhere($table, $column, $value, $operator, 'OR');
    }


    // GroupBy OderBy

    /**
     * Alias for groupBy().
     *
     * Provided for consistency with `order()`. No additional logic is applied.
     *
     * Example:
     *   $db->group('status'); // same as groupBy('status')
     */
    public function group(string $field): self
    {
        return $this->groupBy($field);
    }

    /**
     * Adds a GROUP BY clause to the query.
     * Supports multiple calls for chaining multiple GROUP BY fields.
     *
     * @param string $groupByField The database field or expression to group by.
     *
     * @return static Returns the current instance for method chaining.
     * @throws \Throwable
     */
    public function groupBy(string $groupByField): self
    {
        $this->_secureValidateSqlExpression($groupByField);
        $this->_groupBy[] = $groupByField;

        return $this;
    }

    /**
     * Checks if at least one record exists in the table that satisfies the previously set WHERE conditions.
     * The WHERE conditions must be set before calling this method using `where()`.
     *
     * @param string $tableName The name of the database table to check.
     *
     * @return bool True if at least one matching record exists, false otherwise.
     * @throws \Exception If an error occurs during the query.
     * @throws \Throwable
     */
    public function has(string $tableName): bool
    {
        $this->getOne($tableName, '1');
        return $this->_count >= 1;
    }

    /**
     * Alias for orderBy() with ASC as default direction.
     *
     * This method defaults to ascending order (ASC) unless 'desc' is explicitly passed.
     * It supports all parameters of orderBy(), including custom field lists or regex for advanced sorting.
     *
     * Example:
     *   $db->order('id');                    // ASC
     *   $db->order('id', 'desc');           // DESC
     *   $db->order('FIELD(status)', '', ['pending', 'completed']); // ASC + custom list
     *
     * Internally delegates to orderBy().
     */

    public function order(string $field, string $direction = '', array|string|null $customFieldsOrRegExp = null): self
    {
        $direction = strtolower(trim($direction));
        $direction = ($direction === 'desc') ? 'DESC' : 'ASC';

        return $this->orderBy($field, $direction, $customFieldsOrRegExp);
    }

    /**
     * Adds an ORDER BY clause to the query.
     *
     * Supports multiple calls for chaining multiple ORDER BY conditions.
     * Allows ordering by a simple field, a set of custom fields (using FIELD()),
     * or by a regular expression (REGEXP).
     *
     * @param string             $orderByField         Database field to order by.
     * @param string             $orderbyDirection     Direction: 'ASC' or 'DESC'. Default: 'DESC'.
     * @param array|string|null  $customFieldsOrRegExp Optional: custom order values (FIELD) or regex pattern (REGEXP).
     *
     * @return static
     * @throws \InvalidArgumentException|\Throwable on invalid inputs.
     */
    public function orderBy(string $orderByField, string $orderbyDirection = 'DESC', array|string|null $customFieldsOrRegExp = null): self
    {
        $allowedDirections = ['ASC', 'DESC'];
        $orderbyDirection = strtoupper(trim($orderbyDirection));

        if (!in_array($orderbyDirection, $allowedDirections, true)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid order direction: {$orderbyDirection}"),
                'orderBy'
            );
        }

        // Sicherheitsfilter auf rohes ORDER BY-Feld anwenden
        $this->_secureValidateSqlExpression($orderByField);

        // Prefixing (nur wenn kein schema.table vorhanden ist)
        $prefix = $this->getPrefix();
        if (!str_contains($orderByField, '.')) {
            $orderByField = $prefix . $orderByField;
        } elseif (preg_match('/^`([a-zA-Z0-9_]+)`\.`([a-zA-Z0-9_]+)`$/', $orderByField, $m)) {
            $orderByField = '`' . $prefix . $m[1] . '`.`' . $m[2] . '`';
        }

        // Spezialfälle: FIELD() oder REGEXP
        if (is_array($customFieldsOrRegExp)) {
            // Sonderfall FIELD()
            $customFieldsOrRegExp = array_map(fn($v) => preg_replace("/[^\x80-\xffa-z0-9\.\(\),_` ]+/i", '', $v), $customFieldsOrRegExp);
            $orderByField = 'FIELD(' . $orderByField . ', "' . implode('","', $customFieldsOrRegExp) . '")';

        } elseif (is_string($customFieldsOrRegExp)) {
            // Sonderfall REGEXP
            if (!preg_match('/^[\w\s\^\$\.\*\+\?\|\[\]\(\)]+$/', $customFieldsOrRegExp)) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Invalid regular expression: {$customFieldsOrRegExp}"),
                    'orderBy'
                );
            }

            $orderByField .= " REGEXP '" . $customFieldsOrRegExp . "'";

        } elseif ($customFieldsOrRegExp !== null) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid custom field or REGEXP value: " . print_r($customFieldsOrRegExp, true)),
                'orderBy'
            );
        }

        $this->_orderBy[$orderByField] = $orderbyDirection;
        return $this;
    }

    // SQL Actions

    /**
     * Creates and returns a deep copy of the current query object.
     *
     * This is useful when you want to reuse the current query setup
     * (conditions, joins, etc.) but make changes to the clone without
     * affecting the original instance.
     *
     * @return static A cloned instance of the current query object
     */
    public function copy(): static
    {
        return unserialize(serialize($this));
    }

    /**
     * Performs a DELETE query. Be sure to call "where" beforehand.
     *
     * @param string $tableName Name of the database table.
     * @param int|array|null $numRows Optional limit for the number of rows to delete.
     *
     * @return bool True on success, false on failure.
     * @throws \Exception When used inside a subquery context.
     * @throws \Throwable
     */
    public function delete(string $tableName, int|array|null $numRows = null): bool
    {
        if ($this->isSubQuery) {
            return false;
        }

        $this->_count = 0;

        $tableName = $this->_secureValidateTable($tableName);
        $this->_tableName = $this->getPrefix() . $tableName;
        $this->_query = 'DELETE FROM ' . $this->_tableName . ' ';

        try {
            $stmt = $this->_buildQuery($numRows);

            $this->logQuery($this->_query, $this->_bindParams);
            $stmt->execute();

            $this->_count = $stmt->rowCount();
            $this->countQuery();
            return true;
        } catch (\PDOException $e) {
            return $this->handleException($e, 'delete');
        } finally {
            $this->reset(true);
        }
    }

    /**
     * Performs a SELECT query on a given table.
     *
     * @param string $tableName The table to query (prefix is applied automatically).
     * @param int|array|null $numRows Limit as [offset, count] or just count.
     * @param string|array $columns Columns to select (default: '*').
     * @return bool|self|array|string Returns result set or false on failure, string for json return
     * @throws \Throwable
     */
    public function get(string $tableName, int|array $numRows = null, string|array $columns = '*'): bool|self|array|string
    {
        $this->_count = 0;
        $this->_totalCount = 0;

        $tableName = $this->_secureValidateTable($tableName);

        if (preg_match('/^([^\s]+)\s+([^\s]+)$/', trim($tableName), $m)) {
            $table = $m[1];
            $alias = $m[2];
            $this->_tableAlias = $alias;
        } else {
            $table = trim($tableName);
            $alias = null;
            $this->_tableAlias = null;
        }

        $prefixed = $this->getPrefix() . $table;
        $this->_tableName = $alias ? "{$prefixed} {$alias}" : $prefixed;

        $columnList = is_array($columns) ? implode(', ', $columns) : $columns;
        $this->_query = 'SELECT ' . implode(' ', $this->_queryOptions) . ' ' . $columnList . ' FROM ' . $this->_tableName;

        // Aliase extrahieren, falls vorhanden
        $this->_selectAliases = $this->_extractAliasesFromSelect($columnList);

        // Dynamische Bestandteile anhängen
        $this->_buildJoin();
        $this->_secureSanitizeWhere($this->_where);
        $this->_buildCondition('WHERE', $this->_where);
        $this->_buildGroupBy();
        $this->_secureSanitizeHaving($this->_pendingHaving, $this->_selectAliases);
        $this->_buildCondition('HAVING', $this->_having);
        $this->_buildOrderBy();
        $this->_buildLimit($numRows);

        if ($this->_forUpdate) {
            $this->_query .= ' FOR UPDATE';
        } elseif ($this->_lockInShareMode) {
            $this->_query .= ' LOCK IN SHARE MODE';
        }

        if ($this->isSubQuery) {
            return $this;
        }

        try {
            $this->_lastQuery = $this->_replacePlaceHolders($this->_query, $this->_bindParams);
            $pdo = $this->connect($this->defConnectionName);
            $stmt = $pdo->prepare($this->_query);
            $stmt->execute($this->_getBindValues());

            $this->_count = $stmt->rowCount();
            $error = $stmt->errorInfo();
            if (!empty($error[1])) {
                $e = new \PDOException($error[2], (int)$error[1]);
                $this->handleException($e, 'get');
            }
            $rows = $stmt->fetchAll($this->getPdoFetchMode());

            if (in_array('SQL_CALC_FOUND_ROWS', $this->_queryOptions)) {
                $this->_totalCount = (int) $pdo->query("SELECT FOUND_ROWS()")->fetchColumn();
            }

            return $this->_mapResultByKey($rows);
        } catch (\PDOException $e) {
            return $this->handleException($e, 'get');
        } finally {
            if (!$this->isSubQuery) {
                $this->reset(true);
            }
        }
    }

    /**
     * Returns only the first row from a select query.
     *
     * @param string $tableName The table name.
     * @param string|array $columns Columns to select (default: '*').
     * @return array|null|string      The first row as array, or JSON string if output mode is 'json'
     * @throws \Throwable
     */
    public function getOne(string $tableName, string|array $columns = '*'): array|string|bool
    {
        if ($this->isSubQuery) {
            $this->reset(true);
            throw $this->handleException(
                new \LogicException("getOne() must not be called inside a subQuery() context."),
                'getOne'
            );
        }

        if (!is_string($columns) && !is_array($columns)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid \$columns argument for getOne(): must be string or array."),
                'getOne'
            );
        }

        $res = $this->get($tableName, 1, $columns);

        if (is_array($res)) {
            foreach ($res as $row) {
                $this->countQuery();
                return $row;
            }
        }

        if (is_string($res)) {
            $this->countQuery();
            return $res;
        }
        $this->countQuery();
        return false;
    }

    /**
     * Returns a single column value (or array of values) from a table.
     * If $limit == 1, returns the value directly. Otherwise, returns an array of values.
     *
     * @param string $tableName Table name.
     * @param string $column Column to select.
     * @param int|null $limit Limit rows (default: 1). If >1, returns array.
     * @return mixed|null         The value(s) or null if not found.
     * @throws \Exception On query failure.
     * @throws \Throwable
     */
    public function getValue(string $tableName, string $column, ?int $limit = 1): mixed
    {
        if ($this->getReturnMode() === 'json') {
            $this->reset(true);
            throw $this->handleException(
                new \RuntimeException("getValue() does not support JSON return mode."),
                'getValue'
            );
        }

        $res = $this->arrayBuilder()->get($tableName, $limit, "{$column} AS retval");

        if (empty($res)) {
            $this->countQuery();
            return null;
        }

        if ($limit === 1) {
            $first = reset($res);
            $this->countQuery();
            return $first['retval'] ?? null;
        }

        $values = [];
        foreach ($res as $row) {
            $values[] = $row['retval'] ?? null;
        }
        $this->countQuery();
        return $values;
    }

    /**
     * Inserts a new row into a table.
     *
     * @param string $tableName   Name of the table.
     * @param array  $insertData  Associative array of column => value.
     * @return int Inserted row ID (if auto-increment), or 1 if no auto-increment column exists.
     * @throws \PDOException on failure.
     * @throws \Exception
     * @throws \Throwable
     */
    public function insert(string $tableName, array $insertData): int
    {
        $this->_count = 0;
        $tableName = $this->_secureValidateTable($tableName);
        $insertData = $this->_secureValidateInsertValues($insertData);
        return $this->_buildInsert($tableName, $insertData, 'INSERT');
    }

    /**
     * Internal helper method for insertMulti().
     *
     * Works identically to insert(), but does NOT reset the internal insert counter.
     * This allows insertMulti() to accumulate the total inserted row count across multiple calls.
     *
     * @param string $tableName The name of the table to insert into.
     * @param array $insertData Associative array of column => value pairs.
     * @return int                Returns the last insert ID, or 1 if no auto-increment column is used.
     * @throws \RuntimeException  If the insert fails due to subquery context or preparation error.
     * @throws \Exception
     * @throws \Throwable
     */
    protected function insertMultis(string $tableName, array $insertData): int
    {
        $tableName = $this->_secureValidateTable($tableName);
        $insertData = $this->_secureValidateInsertValues($insertData);
        return $this->_buildInsert($tableName, $insertData, 'INSERT');
    }

    /**
     * Inserts multiple rows at once (each row triggers a single insert).
     *
     * @param string $tableName The table name.
     * @param array $multiInsertData 2D array of row data (each row is an assoc array or, with $dataKeys, an indexed array).
     * @param array|null $dataKeys Optional: column names (if $multiInsertData uses numeric arrays).
     * @return array|false Returns array of IDs (or 1 if no auto-increment), or false on error (with rollback).
     * @throws \Exception
     * @throws \Throwable
     */
    public function insertMulti(string $tableName, array $multiInsertData, array $dataKeys = null): array|false
    {
        $this->_stmtError = null;
        $this->_stmtErrno = null;
        $this->_count = 0;

        $this->_batchMode[$this->defConnectionName] = true;
        // If not already in transaction, wrap in one for safety
        $autoCommit = !$this->_transaction_in_progress[$this->defConnectionName];
        $ids = [];

        if ($autoCommit) {
            $this->startTransaction();
        }

        foreach ($multiInsertData as $insertData) {
            if ($dataKeys !== null) {

                if (array_values($insertData) === $insertData) {
                    $insertData = array_combine($dataKeys, $insertData);
                    if ($insertData === false) {
                        $this->_stmtError = "Error in array_combine(): number of keys and values do not match.";
                        if ($autoCommit) {
                            $this->rollback();
                        }
                        $this->setManualLastErrors();
                        return false;
                    }
                }
                else {
                    $this->_stmtError = "insertMulti(): dataKeys provided but insertData is already associative.";
                    if ($autoCommit) {
                        $this->rollback();
                    }
                    $this->setManualLastErrors();
                    return false;
                }
            }

            $id = $this->insertMultis($tableName, $insertData);
            if (!$id) {
                $this->_stmtError = $this->_stmtError ?? "insertMulti(): insert() failed.";
                if ($autoCommit) {
                    $this->rollback();
                }
                $this->setManualLastErrors();
                return false;
            }

            $ids[] = $id;
        }

        if ($autoCommit) {
            $this->commit();
        }
        $this->countQuery();
        return $ids;
    }

    /**
     * Performs a bulk insert with multiple rows in a single query.
     *
     * @param string $tableName Name of the table.
     * @param array $multiRows Array of associative arrays, each representing a row.
     * @return int Number of rows inserted.
     * @throws \PDOException on failure.
     * @throws \Throwable
     */
    public function insertBulk(string $tableName, array $multiRows): int
    {
        // Check for illegal values like [F]/[I]/[N] or Subqueries
        if ($this->_checkBulkInput[$this->defConnectionName]){
            foreach ($multiRows as $row) {
                foreach ($row as $value) {
                    if (is_array($value)) {
                        $key = array_key_first($value);
                        if (in_array($key, ['[F]', '[I]', '[N]'], true)) {
                            $this->reset(true);
                            throw $this->handleException(
                                new \Exception("insertBulk() does not support special values like [F], [I], [N]."),
                                'insertBulk'
                            );
                        }
                    } elseif (is_object($value) && method_exists($value, 'getSubQuery')) {
                        $this->reset(true);
                        throw $this->handleException(
                            new \Exception("insertBulk() does not support subqueries."),
                            'insertBulk'
                        );
                    }
                }
            }
        }

        $this->_count = 0;

        if (empty($multiRows)) {
            $this->countQuery();
            return 0;
        }
        $tableName = $this->_secureValidateTable($tableName);
        $columns = array_keys(reset($multiRows));
        $placeholders = '(' . implode(', ', array_fill(0, count($columns), '?')) . ')';
        $allPlaceholders = implode(', ', array_fill(0, count($multiRows), $placeholders));
        $bindParams = [];

        foreach ($multiRows as $row) {
            foreach ($columns as $col) {
                $bindParams[] = $row[$col] ?? null;
            }
        }

        $table = $this->getPrefix() . $tableName;
        $quotedCols = implode(', ', array_map(fn($col) => "`$col`", $columns));
        $sql = "INSERT INTO {$table} ({$quotedCols}) VALUES {$allPlaceholders}";

        $this->_query = $sql;
        $this->_bindParams = $bindParams;

        try {
            $pdo = $this->connect($this->defConnectionName);
            $stmt = $pdo->prepare($sql);
            $stmt->execute($bindParams);

            $this->_count = $stmt->rowCount();
            $this->_lastQuery = $this->_replacePlaceHolders($sql, $bindParams);

            $this->countQuery();
            return $this->_count;
        } catch (\Throwable $e) {
            return $this->handleException($e, 'insertBulk');
        } finally {
            $this->reset(true);
        }
    }

    /**
     * Performs a paginated SELECT query on a given table.
     *
     * Automatically calculates the OFFSET based on the current page and page limit.
     *
     * @param string $table The table name.
     * @param int $page The current page number (1-based).
     * @param array|string $fields The columns to select (default: ['*']).
     *
     * @return array Result set for the given page.
     *
     * @throws \RuntimeException If the table name is invalid.
     * @throws \Throwable
     */
    public function paginate(string $table, int $page, array|string $fields = ['*']): array
    {
        $limit = $this->getPageLimit(); // only one access + log fallback if needed

        $table = $this->_secureValidateTable($table);
        $offset = $limit * max(0, $page - 1);

        $res = $this
            ->withTotalCount()
            ->get($table, [$offset, $limit], $fields);

        $this->_totalPages = (int) ceil($this->_totalCount / $limit);
        return $res;
    }

    /**
     * Executes a SELECT query, with optional LIMIT/OFFSET, and returns the result rows.
     *
     * @param string          $query   The SQL query (should be a SELECT statement).
     * @param int|array|null  $numRows Optional LIMIT or [offset, count] array.
     * @return bool|array|string Result rows (may be mapped or JSON if returnMode is 'json').
     * @throws \PDOException On query error.
     * @deprecated Will be removed in v1.5.x – use rawQuery() instead.
     *
     */
    public function query(string $query, int|array $numRows = null): bool|array|string
    {
        try {

            if ($numRows !== null) {
                if (is_array($numRows)) {
                    [$offset, $count] = $numRows + [0, 0];
                    $query .= " LIMIT " . intval($offset) . ", " . intval($count);
                } else {
                    $query .= " LIMIT " . intval($numRows);
                }
            }

            $this->_query = $query;
            $pdo = $this->connect($this->defConnectionName);

            $stmt = $pdo->prepare($query);
            $stmt->execute();

            $this->_count = $stmt->rowCount();
            $this->_lastQuery = $query;

            $rows = $stmt->fetchAll($this->getPdoFetchMode());

            return $this->_mapResultByKey($rows);

        } catch (\PDOException $e) {
            return $this->handleException($e, 'query');
        } finally {
            $this->reset(true);
        }
    }

    /**
     * Executes a raw, unprepared SQL statement (use with caution).
     * Does not perform any escaping or parameter binding.
     * Only use when absolutely necessary, e.g. for special SQL commands.
     *
     * @param string $query The raw SQL query to execute.
     * @return PDOStatement|bool Returns a PDOStatement for SELECT-type queries,
     *                           or true for successful non-SELECT queries,
     *                           false on failure.
     * @throws \PDOException on error.
     */
    protected function queryUnprepared(string $query): bool|\PDOStatement
    {

        $pdo = $this->connect($this->defConnectionName);
        return $pdo->query($query);
    }

    /**
     * Replaces a row in the given table. Works like insert but uses SQL REPLACE.
     * If a row with the same primary or unique key exists, it will be deleted and replaced.
     *
     * @param string $tableName  The name of the table.
     * @param array  $insertData Associative array of column => value pairs to insert.
     *
     * @return int Returns the last insert ID on success or 0/1 as status.
     * @throws \Exception Throws exception on failure.
     * @throws \Throwable
     */
    public function replace(string $tableName, array $insertData): int
    {
        $this->_count = 0;
        $tableName = $this->_secureValidateTable($tableName);
        $insertData = $this->_secureValidateInsertValues($insertData);
        return $this->_buildInsert($tableName, $insertData, 'REPLACE');
    }

    /**
     * Checks if the given table(s) exist in the currently selected database.
     *
     * @param string|array $tables One or multiple table names to check.
     * @return bool True if all tables exist, false otherwise.
     * @throws \Exception|\Throwable
     */
    public function tableExists(string|array $tables): bool
    {
        $tables = (array)$tables;
        if (empty($tables)) {
            return false;
        }

        $dbName = $this->connectionsSettings[$this->defConnectionName]['db'] ?? null;
        if (!$dbName) {
            $this->reset(true);
            throw $this->handleException(
                new \Exception("No database selected for connection '{$this->defConnectionName}'"),
                'tableExists'
            );
        }

        $prefix = $this->getPrefix();

        $validatedTables = [];
        foreach ($tables as $t) {
            $validated = $this->_secureValidateTable($t);
            $validatedTables[] = $prefix . $validated;
        }

        $placeholders = rtrim(str_repeat('?,', count($validatedTables)), ',');
        $sql = "SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name IN ($placeholders)";

        $params = array_merge([$dbName], $validatedTables);

        try {
            $stmt = $this->connect($this->defConnectionName)->prepare($sql);
            foreach ($params as $i => $val) {
                $stmt->bindValue($i + 1, $val);
            }

            $stmt->execute();
            $result = $stmt->fetch(\PDO::FETCH_ASSOC);

            return (int)($result['count'] ?? 0) === count($validatedTables);
        } catch (\Throwable $e) {
            return $this->handleException($e, 'tableExists');
        }
    }

    /**
     * Performs an UPDATE query. Call "where" before to specify conditions.
     *
     * @param string $tableName Name of the table.
     * @param array $insertData Associative array of column => value pairs.
     * @param int|null $numRows Optional limit on the number of rows to update.
     *
     * @return bool True on success, false on failure.
     * @throws \Throwable If the query fails and the debug level is >= 1.
     */
    public function update(string $tableName, array $insertData, ?int $numRows = null): bool
    {
        if ($this->isSubQuery) {
            return false;
        }
        $this->_count = 0;

        $tableName = $this->_secureValidateTable($tableName);
        $insertData = $this->_secureValidateInsertValues($insertData);

        $this->_tableName = $this->getPrefix() . $tableName;
        $this->_query = 'UPDATE ' . $this->_tableName;

        try {
            $stmt = $this->_buildQuery($numRows, $insertData);
            $this->_expandArrayParams();
            $stmt->execute($this->_bindParams);

            $this->_count = $stmt->rowCount();
            $this->_stmtError = null;
            $this->_stmtErrno = null;
            $this->countQuery();
            return true;
        } catch (\PDOException $e) {
            return $this->handleException($e, 'update');
        } finally {
            $this->reset(true);
        }
    }

    // Raw

    /**
     * Adds table prefix to certain SQL statements.
     * Only applies to simple queries.
     *
     * @param string $query
     * @return string
     */
    protected function rawAddPrefix(string $query): string
    {
        $query = str_replace(PHP_EOL, '', $query);
        $query = preg_replace('/\s+/', ' ', $query);
        preg_match_all("/(from|into|update|join|describe)\s+[`']?([a-zA-Z0-9_-]+)[`']?/i", $query, $matches);

        // Compatible with old behavior (static or non-static prefix, as needed)
        // $prefix = property_exists($this, 'prefix') ? $this->prefix : (self::$prefix ?? '');
        $prefix = $this->getPrefix();
        if (empty($matches[2])) {
            return $query;
        }

        foreach ($matches[2] as $table) {
            if (strpos($table, $prefix) !== 0) {
                $query = preg_replace(
                    "/(\b(from|into|update|join|describe)\s+)([`']?)$table([`']?)/i",
                    "$1$3" . $prefix . "$table$4",
                    $query
                );
            }
        }

        return $query;
    }

    /**
     * Executes a raw SQL query with optional bindings.
     *
     * Automatically adds the configured table prefix. Returns results based on query type:
     * - SELECT/SHOW/EXPLAIN: array of rows (associative/object based on builder)
     * - INSERT: last insert ID or affected rows
     * - UPDATE/DELETE/REPLACE: affected row count
     *
     * @param string     $query       The SQL query to execute
     * @param array|null $bindParams  Optional array of parameters to bind
     * @return mixed Result depends on query type or false on failure
     * @throws \Throwable
     */
    public function rawQuery(string $query, ?array $bindParams = null): mixed
    {
        $query = $this->rawAddPrefix($query);
        $this->_query = $query;
        $this->_lastQuery = $query;
        $pdo = $this->connect($this->defConnectionName);

        try {
            $stmt = $pdo->prepare($query);

            if (!empty($bindParams)) {
                foreach ($bindParams as $key => $value) {
                    $paramKey = is_int($key) ? $key + 1 : $key;
                    $stmt->bindValue($paramKey, $value);
                }
            }

            $stmt->execute();

            $this->_count = $stmt->rowCount();
            $this->_stmtError = null;
            $this->_stmtErrno = null;

            $type = strtoupper(strtok(ltrim($query), " "));

            return match ($type) {
                'SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN' => $this->_mapResultByKey($stmt->fetchAll($this->getPdoFetchMode())),
                'INSERT'                                => $pdo->lastInsertId() ?: $this->_count,
                'UPDATE', 'DELETE', 'REPLACE'           => $this->_count,
                default                                 => $this->_mapResultByKey($stmt->fetchAll($this->getPdoFetchMode())),
            };
        } catch (\PDOException $e) {
            return $this->handleException($e, 'rawQuery');
        } finally {
            $this->reset(true);
        }
    }


    /**
     * Executes a raw SQL query and returns only the first result row.
     * Note: Does not automatically add 'LIMIT 1' to the query!
     * Behaves like getOne().
     *
     * @param string $query SQL query to execute (can use ? or named placeholders).
     * @param array|null $bindParams Parameters to bind (positional or named).
     * @return array|null Returns the first result row as array, or null if no row found.
     * @throws \PDOException on error.
     * @throws \Throwable
     */
    public function rawQueryOne(string $query, array $bindParams = null): mixed
    {
        $query .= ' LIMIT 1';

        $res = $this->rawQuery($query, $bindParams);

        if ($res === false || (is_array($res) && count($res) === 0)) {
            return false;
        }

        if (is_string($res)) {
            // OutputMode: json
            return $res;
        }

        return $res[0] ?? false;
    }

    /**
     * Executes a raw SQL query and returns only the first column from all rows.
     * - If 'LIMIT 1' is present in the query, returns a single value.
     * - Otherwise, returns an array of values from the first column of each row.
     * Similar to getValue().
     *
     * @param string $query SQL query to execute (can use ? or named placeholders).
     * @param array|null $bindParams Parameters to bind (positional or named).
     * @return mixed Single value (if LIMIT 1) or array of values (otherwise), or null if no result.
     * @throws \PDOException on error.
     * @throws \Throwable
     */
    public function rawQueryValue(string $query, ?array $bindParams = null): mixed
    {
        $res = $this->rawQuery($query, $bindParams);

        if (!$res || !is_array($res) || !isset($res[0]) || !is_array($res[0])) {
            return null;
        }

        $firstKey = array_key_first($res[0]);
        if ($firstKey === null) {
            return null;
        }

        $isLimitOne = preg_match('/limit\s+1(\s*(--.*)?)?;?$/i', $query);

        if ($isLimitOne) {
            return $res[0][$firstKey];
        }

        $values = [];
        foreach ($res as $row) {
            if (isset($row[$firstKey])) {
                $values[] = $row[$firstKey];
            }
        }

        return $values;
    }

    // Bind & Build Methods

    /**
     * Adds a value to the internal parameter bind list, along with its PDO type.
     * Used internally for prepared statement parameter binding.
     *
     * @param mixed $value The value to be bound.
     * @return void
     */
    protected function _bindParam(mixed $value): void
    {
        if (is_array($value) && isset($value['value'], $value['type'])) {
            $this->_bindParams[] = $value;
            return;
        }

        static $typeMap = [
            'integer' => \PDO::PARAM_INT,
            'boolean' => \PDO::PARAM_BOOL,
            'NULL'    => \PDO::PARAM_NULL,
        ];

        $type = $typeMap[gettype($value)] ?? \PDO::PARAM_STR;

        $this->_bindParams[] = [
            'value' => $value,
            'type'  => $type,
        ];
    }

    /**
     * Helper function to bulk-add variables into bind parameters array.
     *
     * @param array<mixed> $values Array of values to bind.
     * @return void
     */
    protected function _bindParams(array $values): void
    {
        if (empty($values)) {
            return;
        }

        foreach ($values as $value) {
            $this->_bindParam($value);
        }
    }

    // Builder Methods

    /**
     * Builds a SQL condition clause (WHERE or HAVING) with proper binding.
     *
     * @param string $operator   The SQL keyword, e.g. 'WHERE' or 'HAVING'.
     * @param array  $conditions Reference to an array of conditions:
     *                           [ [concat, column, comparison, value], ... ]
     *
     * @throws \InvalidArgumentException On invalid input or missing values.
     * @throws \UnexpectedValueException|\Throwable On invalid subquery structure.
     *
     * @return void
     */
    protected function _buildCondition(string $operator, array &$conditions): void
    {
        if (empty($conditions)) {
            return;
        }

        $this->_query .= ' ' . $operator . ' ';

        foreach ($conditions as [$cond, $column, $comparison, $value]) {
            $this->_query .= $cond ? " {$cond} " : '';
            //$useTicks = !($operator === 'HAVING' && preg_match('/^[A-Z]+\(.+\)$/i', $column));
            // $this->_query .= $useTicks ? $this->_addTicks($column) : $column;
            $this->_query .= $this->_secureEscapeTableAndColumn($column);
            $comp = strtoupper(trim($comparison));

            // SubQuery
            if (is_object($value) && method_exists($value, 'getSubQuery')) {
                $sub = $value->getSubQuery();
                $this->_query .= " {$comp} (" . $sub['query'] . ")";
                $this->_bindParams = array_merge($this->_bindParams, $sub['params']);
                continue;
            }

            // NULL-Vergleiche
            if (is_null($value)) {
                if ($comp === 'IS' || $comp === 'IS NOT') {
                    $this->_query .= " {$comp} NULL";
                } else {
                    $this->reset(true);
                    throw $this->handleException(
                        new \InvalidArgumentException("Invalid NULL operator: {$comp}"),
                        '_buildCondition'
                    );
                }
                continue;
            }

            // [F], [I], [N] Platzhalter (nur 1 erlaubt)
            if (is_array($value) && count($value) === 1) {
                if (isset($value['[F]'])) {
                    $this->_query .= " {$comp} " . implode(', ', array_map([$this, '_secureValidateFunc'], $value['[F]']));
                    continue;
                }
                if (isset($value['[I]'])) {
                    $this->_query .= " {$comp} " . implode(', ', array_map([$this, '_addTicks'], $value['[I]']));
                    continue;
                }
                if (isset($value['[N]'])) {
                    $this->_query .= " {$comp} " . implode(', ', array_map('intval', $value['[N]']));
                    continue;
                }
            }

            // IN / NOT IN
            if (in_array($comp, ['IN', 'NOT IN'], true)) {
                if (!is_array($value) || empty($value)) {
                    $this->reset(true);
                    throw $this->handleException(
                        new \InvalidArgumentException("IN operator requires non-empty array."),
                        '_buildCondition'
                    );
                }

                $placeholders = rtrim(str_repeat('?, ', count($value)), ', ');
                $this->_query .= " {$comp} ({$placeholders})";
                $this->_bindParams($value);
                continue;
            }

            // BETWEEN / NOT BETWEEN
            if (in_array($comp, ['BETWEEN', 'NOT BETWEEN'], true)) {
                if (!is_array($value) || count($value) !== 2) {
                    $this->reset(true);
                    throw $this->handleException(
                        new \InvalidArgumentException("BETWEEN requires exactly 2 values."),
                        '_buildCondition'
                    );
                }
                $this->_query .= " {$comp} ? AND ?";
                $this->_bindParams($value);
                continue;
            }

            // EXISTS / NOT EXISTS
            if (in_array($comp, ['EXISTS', 'NOT EXISTS'], true)) {
                if (!is_object($value) || !method_exists($value, 'getSubQuery')) {
                    $this->reset(true);
                    throw $this->handleException(
                        new \InvalidArgumentException("EXISTS requires valid subquery."),
                        '_buildCondition'
                    );
                }
                $sub = $value->getSubQuery();
                $this->_query .= " {$comp} (" . $sub['query'] . ")";
                $this->_bindParams = array_merge($this->_bindParams, $sub['params']);
                continue;
            }

            // Default: skalarer Vergleich
            $this->_query .= " {$comp} ?";
            $this->_bindParam($value);
        }
    }

    /**
     * Helper method to build SQL fragments for INSERT and UPDATE statements.
     * Handles bindings, subqueries, inline expressions, and special flags.
     *
     * @param array $tableData    Associative array of data for query building.
     * @param array $tableColumns List of keys (columns) to use.
     * @param bool  $isInsert     True = INSERT, false = UPDATE.
     *
     * @throws \Exception|\Throwable If input is invalid or special format fails.
     */
    protected function _buildDataPairs(array $tableData, array $tableColumns, bool $isInsert): void
    {
        foreach ($tableColumns as $column) {
            if (!array_key_exists($column, $tableData)) {

                $this->reset(true);
                throw $this->handleException(
                    new \Exception("Column '{$column}' missing in data array."),
                    '_buildDataPairs'
                );
            }

            $value = $tableData[$column];

            if (!$isInsert) {
                $this->_query .= $this->_addTicks($column) . " = ";
            }

            if (is_object($value) && method_exists($value, 'getSubQuery')) {
                $this->_query .= $this->_buildPair('', $value) . ', ';
            } elseif (!is_array($value)) {
                $this->_bindParam($value);

                $this->_query .= '?, ';
            } else {
                $flag = key($value);
                $val  = $value[$flag];

                switch ($flag) {
                    case '[I]':
                        $this->_query .= $this->_addTicks($column) . " {$val}, ";
                        break;
                    case '[F]':
                        if (is_array($val)) {
                            if (count($val) !== 1 || !is_string($val[0])) {
                                $this->reset(true);
                                throw $this->handleException(
                                    new \Exception("Invalid [F] value format for '{$column}'"),
                                    '_buildDataPairs'
                                );
                            }
                            $expr = $val[0];
                        } elseif (is_string($val)) {
                            $expr = $val;
                        } else {
                            $this->reset(true);
                            throw $this->handleException(
                                new \Exception("Invalid [F] value type for '{$column}'"),
                                '_buildDataPairs'
                            );
                        }

                        // Must match: FUNC [+-] interval N UNIT or FUNC()
                        if (
                            !preg_match('/^(\w+\(\))$/i', $expr) &&
                            !preg_match('/^(\w+)\s*[\+\-]\s*interval\s+\d+\s+(second|minute|hour|day|month|year)$/i', $expr)
                        ) {
                            $this->reset(true);
                            throw $this->handleException(
                                new \Exception("Disallowed [F] SQL expression for '{$column}': {$expr}"),
                                '_buildDataPairs'
                            );
                        }

                        $this->_query .= $expr . ', ';
                        break;
                    case '[Z]':
                        if (!is_array($val) || !isset($val[0])) {
                            $this->reset(true);
                            throw $this->handleException(
                                new \Exception("Invalid [F] format for column '{$column}'"),
                                '_buildDataPairs'
                            );
                        }
                        $this->_query .= $val[0] . ", ";
                        if (!empty($val[1])) {
                            $this->_bindParams($val[1]);
                        }
                        break;

                    case '[N]':
                        $escapedValue = is_string($val)
                            ? $this->_addTicks($val)
                            : $this->_addTicks($column);
                        $this->_query .= '!' . $escapedValue . ', ';
                        break;

                    default:
                        $this->reset(true);
                        throw $this->handleException(
                            new \Exception("Invalid operation flag: {$flag} in column '{$column}'"),
                            '_buildDataPairs'
                        );
                }
            }
        }

        $this->_query = rtrim($this->_query, ', ');
    }

    /**
     * Internal helper to build the GROUP BY part of the SQL query.
     *
     * Appends a GROUP BY clause to the current query if groupings have been defined.
     *
     * @return void
     */
    protected function _buildGroupBy(): void
    {
        if (empty($this->_groupBy)) {
            return;
        }

        $escaped = array_map(function ($field) {
            if (str_contains($field, '(') || str_contains($field, '`')) {
                return $field;
            }
            return $this->_addTicks($field);
        }, $this->_groupBy);

        $this->_query .= ' GROUP BY ' . implode(', ', $escaped);
    }

    /**
     * Internal helper to perform INSERT or REPLACE operations using PDO prepared statements.
     *
     * Automatically adds table prefix and uses positional placeholders (?). Also respects query options.
     * Note: Supports basic `INSERT ... VALUES (...)` — no multi-row, no ON DUPLICATE UPDATE here.
     *
     * @param string $tableName   Table name without prefix.
     * @param array  $insertData  Associative array of column => value.
     * @param string $operation   SQL operation to perform: 'INSERT' or 'REPLACE'.
     *
     * @return int|bool Insert ID (int) on success with auto-increment, true if no ID, false on error.
     * @throws \Exception|\Throwable If called within subquery context or preparation fails.
     */
    protected function _buildInsert(string $tableName, array $insertData, string $operation): int|bool
    {

        if ($this->isSubQuery) {
            return false;
        }

        $prefix = $this->getPrefix();
        $table  = $prefix . $tableName;
        $columns = [];
        $placeholders = [];
        $this->_bindParams = [];

        foreach ($insertData as $col => $val) {
            $columns[] = $this->_addTicks($col);

            // Sonderwert: [F], [I], [N]
            if (is_array($val) && count($val) === 1) {
                $key = array_key_first($val);
                if (in_array($key, ['[F]', '[I]', '[N]'], true)) {
                    $expr = $val[$key];
                    if (is_array($expr)) {
                        $placeholders[] = implode(', ', array_map('trim', $expr));
                    } else {
                        $placeholders[] = trim($expr);
                    }
                    continue;
                }
            }

            // Subquery-Objekt
            if (is_object($val) && method_exists($val, 'getSubQuery')) {
                $placeholders[] = (string) $val;
                continue;
            }

            // Normalwert → gebindet
            $placeholders[] = '?';
            $this->_bindParams[] = $val;
        }

        $columnsStr     = implode(', ', $columns);
        $placeholdersStr = implode(', ', $placeholders);
        $options        = !empty($this->_queryOptions) ? implode(' ', $this->_queryOptions) . ' ' : '';

        $this->_query = sprintf('%s %sINTO %s (%s) VALUES (%s)', $operation, $options, $table, $columnsStr, $placeholdersStr);

        if (!empty($this->_updateColumns)) {
            $this->_buildOnDuplicate($insertData);
        }

        try {
            $pdo = $this->connect($this->defConnectionName);
            $stmt = $pdo->prepare($this->_query);

            foreach ($this->_bindParams as $idx => $value) {
                if (is_array($value) && isset($value['value'], $value['type'])) {
                    $stmt->bindValue($idx + 1, $value['value'], $value['type']);
                } else {
                    $stmt->bindValue($idx + 1, $value);
                }
            }

            $this->logQuery($this->_query, $this->_bindParams);
            $stmt->execute();

            $this->_count = $stmt->rowCount();
            $this->_stmtError = null;
            $this->_stmtErrno = null;

            $haveOnDuplicate = !empty($this->_updateColumns);

            if ($this->_count < 1) {
                $this->countQuery();
                return (bool)$haveOnDuplicate;
            }
            $this->countQuery();
            return ($operation === 'INSERT' && $pdo->lastInsertId()) ? (int) $pdo->lastInsertId() : true;

        } catch (\Exception $e) {
            return $this->handleException($e, '_buildInsert');
        } finally {
            $this->reset(true);
        }
    }

    /**
     * Internal method that builds the INSERT, REPLACE, or UPDATE part of the SQL query.
     * Delegates detailed value handling to _buildDataPairs().
     *
     * @param array $tableData Associative array of column => value pairs.
     *
     * @throws \InvalidArgumentException If the data array is empty.
     * @throws \RuntimeException If the current query type is not recognized.
     * @throws \Exception|\Throwable On internal query formatting issues.
     */

    protected function _buildInsertQuery(?array $tableData): void
    {
        if (empty($tableData)) {
            return;
        }

        $queryType = strtoupper(strtok(trim($this->_query), ' '));
        $isInsert = in_array($queryType, ['INSERT', 'REPLACE'], true);

        if (!$isInsert && $queryType !== 'UPDATE') {
            $this->reset(true);
            throw $this->handleException(
                new \RuntimeException("Unsupported query type in _buildInsertQuery(): '{$queryType}'"),
                '_buildInsertQuery'
            );
        }

        $dataColumns = array_keys($tableData);
        $escapedColumns = [];

        foreach ($dataColumns as $col) {
            $escapedColumns[] = $this->_addTicks($col);
        }

        if ($isInsert) {
            $this->_query .= ' (' . implode(', ', $escapedColumns) . ') VALUES (';
        } else {
            $this->_query .= ' SET ';
        }

        $this->_buildDataPairs($tableData, $dataColumns, $isInsert);

        if ($isInsert) {
            $this->_query .= ')';
        }
    }

    /**
     * Builds the JOIN clauses for the current query.
     *
     * Iterates over all pending JOIN definitions, appends JOIN statements
     * including the JOIN type, table, and ON condition.
     *
     * Also appends any additional WHERE conditions tied specifically to the joined tables
     * (added via joinWhere or joinOrWhere), supporting NULL comparisons and parameter binding.
     *
     * This method is called internally when assembling the final SQL query.
     *
     * @return void
     * @throws \Throwable
     */
    protected function _buildJoin(): void
    {
        if (empty($this->_pendingJoins)) {
            return;
        }

        foreach ($this->_pendingJoins as [$joinType, $joinTable, $joinCondition]) {
            if (is_object($joinTable)) {
                $joinStr = $this->_buildPair('', $joinTable);
            } else {
                $joinStr = $joinTable;

                // Alias extrahieren und in _joinAliases speichern
                if (preg_match('/^(\w+)\s+(\w+)$/', trim($joinTable), $m)) {
                    // e.g. "orders o"
                    $tableName = $m[1];
                    $alias     = $m[2];
                    $this->_joinAliases[$tableName] = $alias;
                } elseif (preg_match('/^(\w+)$/', trim($joinTable), $m)) {
                    // e.g. "users"
                    $tableName = $m[1];
                    $this->_joinAliases[$tableName] = $tableName;
                }
            }

            $this->_query .= " {$joinType} JOIN {$joinStr}";
            $this->_query .= (stripos($joinCondition, 'using') !== false) ? " {$joinCondition}" : " ON {$joinCondition}";

            $key = is_string($joinTable) ? trim($joinTable) : '';
            if (!empty($this->_joinWheres[$key])) {
                foreach ($this->_joinWheres[$key] as [$concat, $column, $operator, $value]) {
                    $col = str_contains($column, '.') ? $this->_secureEscapeTableAndColumn($column) : "`{$column}`";
                    $this->_query .= " {$concat} {$col}";
                    $this->_query .= ($value === null || $value === 'DBNULL') ? " {$operator} NULL" : " {$operator} ?";
                    if ($value !== null && $value !== 'DBNULL') {
                        $this->_bindParam($value);
                    }
                }
            }
        }
    }

    /**
     * Internal helper to build the LIMIT clause of the SQL query.
     *
     * Accepts either a single integer for row count, or an array [offset, count].
     *
     * @param int|array|null $numRows Optional row limit or offset/count pair.
     * @return void
     * @throws \Throwable
     */
    protected function _buildLimit(int|array|null $numRows): void
    {
        if ($numRows === null) {
            return;
        }

        if (is_array($numRows)) {
            if (count($numRows) !== 2) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException('Limit array must contain exactly two elements: [offset, count]'),
                    '_buildLimit'
                );
            }

            [$offset, $count] = array_map('intval', $numRows);

            if ($count < 0 || $offset < 0) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException('Offset and limit must be non-negative integers'),
                    '_buildLimit'
                );
            }

            $this->_query .= " LIMIT {$offset}, {$count}";
        } else {
            $limit = (int) $numRows;

            if ($limit < 0) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException('Limit must be a non-negative integer'),
                    '_buildLimit'
                );
            }

            $this->_query .= " LIMIT {$limit}";
        }
    }

    /**
     * Appends ON DUPLICATE KEY UPDATE clause to current query.
     *
     * @param array $tableData Full insert/update data (used to extract update values)
     * @return void
     * @throws \Exception
     * @throws \Throwable
     */
    protected function _buildOnDuplicate(?array $tableData): void
    {
        if (empty($this->_updateColumns) || !is_array($this->_updateColumns)) {
            return;
        }

        $this->_query .= " ON DUPLICATE KEY UPDATE ";

        if (!empty($this->_lastInsertId)) {
            $safeColumn = $this->_addTicks($this->_lastInsertId);
            $this->_query .= "{$safeColumn} = LAST_INSERT_ID({$safeColumn}), ";
        }

        $updatePairs = [];

        foreach ($this->_updateColumns as $key => $val) {
            if (is_int($key)) {
                $column = $val;
                $updatePairs[$column] = is_array($tableData) && array_key_exists($column, $tableData)
                    ? $tableData[$column]
                    : null;
            } else {
                $updatePairs[$key] = $val;
            }
        }

        $this->_buildDataPairs($updatePairs, array_keys($updatePairs), false);
    }

    /**
     * Internal helper to build the ORDER BY part of the SQL query.
     *
     * Appends an ORDER BY clause if any ordering has been defined.
     * Special case: "RAND()" is treated as a random sort request.
     *
     * @return void
     */
    protected function _buildOrderBy(): void
    {
        if (empty($this->_orderBy)) {
            return;
        }

        $clauses = [];

        foreach ($this->_orderBy as $column => $direction) {
            $direction = strtoupper(trim($direction));

            if (strtolower(str_replace(' ', '', $column)) === 'rand()') {
                $clauses[] = 'RAND()';
                continue;
            }

            // Skip escaping for raw expressions
            $escapedColumn = (str_contains($column, '(') || str_contains($column, '`'))
                ? $column
                : $this->_addTicks($column);

            $clauses[] = "$escapedColumn $direction";
        }

        $this->_query .= ' ORDER BY ' . implode(', ', $clauses) . ' ';
    }

    /**
     * Builds a SQL condition pair for WHERE/HAVING clauses.
     *
     * - If a scalar value is provided, adds it as a bound parameter (`?`) and returns a placeholder expression.
     * - If an object is passed, it must implement a `getSubQuery()` method which returns:
     *   ['query' => '...', 'params' => [...], 'alias' => 'AS x']
     *   In this case, the subquery SQL is embedded and its parameters are appended to the bindings.
     *
     * Example:
     *   $db->_buildPair('=', 5); // returns " = ? " and binds 5
     *   $db->_buildPair('IN', $subQueryObj); // returns " IN (SELECT ...) " and binds subquery params
     *
     * @param string $operator SQL comparison operator (e.g. '=', 'IN', '>', etc.).
     * @param mixed  $value    A scalar value or an object representing a subquery.
     *
     * @return string SQL fragment for the condition.
     * @throws \InvalidArgumentException If a subquery object lacks getSubQuery().
     * @throws \UnexpectedValueException|\Throwable If subquery structure is invalid.
     */
    protected function _buildPair(string $operator, mixed $value,bool $wrap = true): string
    {
        if (!is_object($value)) {
            $this->_bindParam($value);
            return ' ' . $operator . ' ? ';
        }

        if (!method_exists($value, 'getSubQuery')) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException('Subquery object must implement getSubQuery method'),
                '_buildPair'
            );
        }

        $subQuery = $value->getSubQuery();

        if (!is_array($subQuery) || !isset($subQuery['query'], $subQuery['params'])) {
            $this->reset(true);
            throw $this->handleException(
                new \UnexpectedValueException('Subquery must return array with keys: query, params'),
                '_buildPair'
            );
        }

        $this->_bindParams($subQuery['params']);

        $sql = ($wrap ? ' ' . $operator . ' (' : ' ');
        $sql .= $subQuery['query'];
        $sql .= ($wrap ? ')' : '');

        return $sql . (isset($subQuery['alias']) ? ' ' . $subQuery['alias'] : '');
    }

    /**
     * Builds the initial SELECT part of the query with columns and table name.
     *
     * Does nothing if the query is already set.
     *
     * @return void
     */
    protected function _buildTable(): void
    {
        if (!empty($this->_query)) {
            return;
        }

        $select = implode(', ', $this->_queryOptions ?: ['*']);
        $this->_query = "SELECT {$select} FROM {$this->_tableName}";
    }

    /**
     * Builds and prepares a full SQL statement with all components like WHERE, JOIN, etc.
     *
     * @param int|array|null $numRows   LIMIT clause. Either integer (count) or array [offset, count].
     * @param array|null     $tableData Data for UPDATE or INSERT (optional).
     * @return \PDOStatement|string Prepared PDO statement.
     * @throws \Exception|\Throwable On preparation failure or invalid context.
     */
    protected function _buildQuery(int|array|null $numRows = null, array $tableData = null): \PDOStatement|string
    {
        // Tabellenname + JOINs vorbereiten
        $this->_buildTable();
        $this->_buildJoin();
        // Build INSERT/UPDATE SET-Teil, wenn $tableData gesetzt
        $this->_buildInsertQuery($tableData);
        // WHERE prüfen + aufbauen
        $this->_secureSanitizeWhere($this->_where);
        $this->_buildCondition('WHERE', $this->_where);

        // Nur bei SELECT: GROUP BY, HAVING, ORDER BY
        $isSelect = str_starts_with(trim($this->_query), 'SELECT');

        if ($isSelect) {
            $this->_buildGroupBy();

            if (!empty($this->_pendingHaving)) {
                if (empty($this->_selectAliases)) {
                    $selectClause = $this->_extractSelectClause($this->_query);
                    $this->_selectAliases = $this->_extractAliasesFromSelect($selectClause);
                }
                $this->_secureSanitizeHaving($this->_pendingHaving, $this->_selectAliases);
                $this->_pendingHaving = [];
            }

            $this->_buildCondition('HAVING', $this->_having);
            $this->_buildOrderBy();
        }

        // LIMIT für alle Querytypen erlaubt (auch UPDATE/DELETE)
        $this->_buildLimit($numRows);

        // ON DUPLICATE nur bei INSERT relevant
        $this->_buildOnDuplicate($tableData);

        // Lock-Modi bei SELECT
        if ($this->_forUpdate) {
            $this->_query .= ' FOR UPDATE';
        }
        if ($this->_lockInShareMode) {
            $this->_query .= ' LOCK IN SHARE MODE';
        }

        // Expand array params before preparing/binding
        $this->_expandArrayParams();

        // Debug: Query mit Werten speichern
        $this->_lastQuery = $this->_replacePlaceHolders($this->_query, $this->_bindParams);

        // Subqueries geben nur SQL zurück, kein Statement
        if ($this->isSubQuery) {
            return $this->_query;
        }

        // Statement vorbereiten
        try {
            $pdo = $this->connect($this->defConnectionName);
            $stmt = $pdo->prepare($this->_query);

            foreach ($this->_bindParams as $idx => $bind) {
                // Normalize parameter identifier
                $param = is_int($idx)
                    ? $idx + 1
                    : (str_starts_with((string)$idx, ':') ? (string)$idx : ':' . (string)$idx);

                // Extract value (+ optional type)
                if (is_array($bind)) {
                    $val  = array_key_exists('value', $bind) ? $bind['value']
                        : (array_key_exists(0, $bind) ? $bind[0] : null);
                    $type = $bind['type'] ?? null;
                } else {
                    $val  = $bind;
                    $type = null;
                }

                // Scalarize/validate
                if ($val instanceof \DateTimeInterface) {
                    $val = $val->format('Y-m-d H:i:s');
                }
                if (is_array($val) || (is_object($val) && !($val instanceof \Stringable))) {
                    throw $this->handleException(
                        new \InvalidArgumentException('Non-scalar value in bind parameters.'),
                        __METHOD__
                    );
                }

                // Bind (with type if provided)
                if ($type !== null) {
                    $stmt->bindValue($param, $val, $type);
                } else {
                    $stmt->bindValue($param, $val);
                }
            }

            return $stmt;
        } catch (\PDOException $e) {
            $this->reset(true);
            throw $this->handleException(
                new \Exception("Failed to prepare query: " . $e->getMessage(), (int)$e->getCode()),
                '_buildQuery'
            );
        }
    }

    /**
     * Ensures proper backtick quoting for SQL identifiers.
     * Automatically adds backticks to unquoted identifiers,
     * including dot-notation such as table.column or db.table.column.
     * Leaves expressions, aliases, and already quoted identifiers unchanged.
     *
     * @param string $field The SQL identifier or expression.
     * @return string The properly quoted identifier.
     */
    protected function _addTicks(string $field): string
    {
        if (preg_match('/\b(AS|IN|BETWEEN|CASE|WHEN|IF|NULL|IS|NOT|LIKE)\b/i', $field)) {
            return $field;
        }

        if (preg_match('/^`[^`]+`$/', $field)) {
            return $field;
        }

        if (preg_match('/^(`[^`]+`\.)+`[^`]+`$/', $field)) {
            return $field;
        }

        $parts = explode('.', $field);

        foreach ($parts as &$part) {
            if (!str_starts_with($part, '`') || !str_ends_with($part, '`')) {
                $part = '`' . str_replace('`', '', $part) . '`';
            }
        }

        return implode('.', $parts);
    }

    /**
     * Expand array parameters in the current query for IN (...) and similar constructs.
     *
     * Rules:
     * - Detects named (e.g., :ids) vs. positional (?) placeholders; mixed usage is rejected.
     * - Named:
     *     - Scalars: bind once; multiple uses of the same placeholder in SQL are allowed.
     *     - Arrays: each SQL occurrence of :name expands to a unique list (:name_OccIdx_ElemIdx).
     *       Bind keys are stored WITHOUT the leading colon.
     * - Positional:
     *     - Each array parameter expands the next '?' into N comma-separated '?'.
     * - Empty arrays are rejected.
     * - On any error: reset state and rethrow via handleException(...).
     */
    protected function _expandArrayParams(): void
    {
        if (empty($this->_bindParams)) {
            return;
        }

        try {
            $query    = (string) $this->_query;
            $hasNamed = (bool) preg_match('/:[a-zA-Z_][a-zA-Z0-9_]*/', $query);
            $hasPos   = substr_count($query, '?') > 0;

            if ($hasNamed && $hasPos) {
                throw new \InvalidArgumentException('Mixed named and positional placeholders are not supported.');
            }

            if ($hasNamed) {
                $newParams  = [];
                $normalized = [];

                // Normalize to name => ['value'=>..., 'type'=>...]
                foreach ($this->_bindParams as $k => $p) {
                    if (is_array($p) && array_key_exists('name', $p)) {
                        $name  = ltrim((string) $p['name'], ':');
                        $value = $p['value'] ?? null;
                        $type  = $p['type']  ?? null;
                    } elseif (is_array($p) && count($p) === 1) {
                        $origKey = (string) array_key_first($p);
                        $name    = ltrim($origKey, ':');
                        $value   = $p[$origKey];
                        $type    = null;
                    } elseif (is_string($k)) {
                        $name  = ltrim($k, ':');
                        $value = $p;
                        $type  = null;
                    } else {
                        throw new \InvalidArgumentException('Positional parameter provided in named placeholder mode.');
                    }

                    if (array_key_exists($name, $normalized)) {
                        throw new \InvalidArgumentException("Duplicate bind for :{$name}");
                    }
                    $normalized[$name] = ['value' => $value, 'type' => $type];
                }

                $occurrence = [];
                $pattern    = '/(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)\b/';

                $query = preg_replace_callback(
                    $pattern,
                    function ($m) use (&$occurrence, $normalized, &$newParams) {
                        $name = $m[1];

                        if (!array_key_exists($name, $normalized)) {
                            throw new \InvalidArgumentException("No bind value provided for :{$name}");
                        }

                        $entry = $normalized[$name];
                        $value = $entry['value'];

                        $idx = $occurrence[$name] ?? 0;
                        $occurrence[$name] = $idx + 1;

                        if (is_array($value)) {
                            $count = count($value);
                            if ($count === 0) {
                                throw new \InvalidArgumentException("Empty array provided for placeholder :{$name}");
                            }

                            $expanded = [];
                            for ($i = 0; $i < $count; $i++) {
                                $phSql = sprintf(':%s_%d_%d', $name, $idx, $i);
                                $phKey = ltrim($phSql, ':'); // bind key without colon
                                $expanded[]        = $phSql;
                                $newParams[$phKey] = $value[$i];
                            }
                            return implode(', ', $expanded);
                        }

                        // Scalar: bind once; allow multiple SQL uses
                        $bindKey = $name; // without colon
                        if (!array_key_exists($bindKey, $newParams)) {
                            $newParams[$bindKey] = $value;
                        }
                        return ':' . $name;
                    },
                    $query
                );

                $this->_query      = $query;
                $this->_bindParams = $newParams;
                return;
            }

            // Positional mode
            $newQuery  = $query;
            $newParams = [];

            foreach ($this->_bindParams as $param) {
                $value = (is_array($param) && array_key_exists('value', $param)) ? $param['value'] : $param;

                if (is_array($value)) {
                    $count = count($value);
                    if ($count === 0) {
                        throw new \InvalidArgumentException('Empty array provided for positional placeholder.');
                    }

                    $pos = strpos($newQuery, '?');
                    if ($pos === false) {
                        throw new \RuntimeException('Not enough positional placeholders (?) in query for provided parameters.');
                    }

                    $placeholders = implode(', ', array_fill(0, $count, '?'));
                    $newQuery     = substr_replace($newQuery, $placeholders, $pos, 1);

                    foreach ($value as $v) {
                        $newParams[] = $v;
                    }
                } else {
                    $newParams[] = $value;
                }
            }

            if (substr_count($newQuery, '?') !== count($newParams)) {
                throw new \RuntimeException('Placeholder/parameter count mismatch after expansion.');
            }

            $this->_query      = $newQuery;
            $this->_bindParams = $newParams;

        } catch (\Throwable $e) {
            $this->reset(true);
            throw $this->handleException($e, __METHOD__);
        }
    }


    protected function _extractSelectClause(string $query): string
    {
        if (preg_match('/SELECT\s+(.*?)\s+FROM\s/i', $query, $m)) {
            return $m[1];
        }
        return '*';
    }
    protected function _extractAliasesFromSelect(string|array $columns): array
    {
        $aliases = [];

        if (is_array($columns)) {
            foreach ($columns as $col) {
                if (preg_match('/\s+AS\s+([a-zA-Z0-9_]+)/i', $col, $m)) {
                    $aliases[] = $m[1];
                } elseif (preg_match('/([a-zA-Z0-9_]+)$/', $col, $m) && str_contains($col, '(')) {
                    $aliases[] = $m[1];
                }
            }
        } elseif (is_string($columns)) {
            foreach (explode(',', $columns) as $col) {
                if (preg_match('/\s+AS\s+([a-zA-Z0-9_]+)/i', $col, $m)) {
                    $aliases[] = trim($m[1]);
                } elseif (preg_match('/([a-zA-Z0-9_]+)$/', $col, $m) && str_contains($col, '(')) {
                    $aliases[] = trim($m[1]);
                }
            }
        }

        return array_unique($aliases);
    }

    /**
     * Gibt nur die Werte aus _bindParams zurück, um sie an execute() zu übergeben.
     *
     * @return array<int, mixed>
     */
    protected function _getBindValues(): array
    {
        return array_map(
            fn($p) => is_array($p) && array_key_exists('value', $p) ? $p['value'] : $p,
            $this->_bindParams
        );
    }

    protected function _isInGroupBy(string $column): bool
    {
        foreach ($this->_groupBy as $group) {
            if (trim($group, '`') === trim($column, '`')) {
                return true;
            }
        }
        return false;
    }

    protected function _isAggregateFunction(string $expr): bool
    {
        // Todo: evtl. noch erweitern?
        return (bool) preg_match('/^(SUM|AVG|COUNT|MIN|MAX)\s*\(/i', $expr);
    }

    /**
     * Applies returnKey-based mapping to a result set, if enabled.
     *
     * If returnKey is set and returnMode is not 'json',
     * the result rows are mapped using the returnKey column as array key
     * (e.g. ['id' => row, ...]).
     *
     * If returnMode is 'json', returnKey is ignored for performance reasons:
     * the original result is returned as-is (JSON-encoded) without remapping.
     *
     * If no returnKey is set, the result is returned unchanged.
     *
     * @param array $rows List of DB result rows (array or object entries)
     * @return array|string|object Remapped result, original rows, or JSON string
     * @throws \RuntimeException|\Throwable If returnKey is set but not found in a result row
     */
    protected function _mapResultByKey(array $rows): array|string|object
    {
        $mode = $this->getReturnMode();
        // If return mode is set to 'json' and a returnKey is also defined,
        // the returnKey is considered obsolete. Re-mapping the result set
        // just to re-encode it into JSON would be inefficient — especially for large datasets.
        // That would require converting to an array, sorting it by key, and
        // then converting it back to JSON. This might be refactored in the future,
        // but for now there's no compelling reason to add this overhead.
        if ($mode === 'json') {
            $this->countQuery();
            return json_encode($rows, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
        }

        $key = $this->getReturnKey();
        // Kein Mapping erforderlich
        if ($key === null) {
            $this->countQuery();
            return $rows;
        }

        $mapped = [];
        foreach ($rows as $i => $row) {
            if (is_object($row)) {
                $row = (array) $row;
            }
            if (!array_key_exists($key, $row)) {
                $this->reset(true);
                throw $this->handleException(
                    new \RuntimeException("ReturnKey '{$key}' not found in result row #{$i}"),
                    '_mapResultByKey'
                );
            }
            $mapped[$row[$key]] = $row;
        }
        $this->countQuery();
        return $mapped;
    }

    /**
     * Debug helper: replaces ? placeholders in SQL string for logging purposes only.
     * NEVER use this result to execute SQL!
     *
     * @param string $query  The SQL query with `?` placeholders.
     * @param array  $params The values to inject into the placeholders.
     * @return string The SQL string with values interpolated.
     */
    protected function _replacePlaceHolders(string $query, array $params): string
    {
        $parts = explode('?', $query);
        $result = '';
        $count = count($parts) - 1;

        foreach ($parts as $i => $part) {
            $result .= $part;

            if ($i < $count) {
                $value = $params[$i] ?? null;

                switch (true) {
                    case is_string($value):
                        $result .= "'" . addslashes($value) . "'";
                        break;

                    case is_null($value):
                        $result .= "NULL";
                        break;

                    case is_bool($value):
                        $result .= $value ? '1' : '0';
                        break;

                    case is_array($value):
                        if (array_key_exists('value', $value)) {
                            $inner = $value['value'];
                            $result .= match (true) {
                                is_string($inner) => "'" . addslashes($inner) . "'",
                                is_null($inner) => "NULL",
                                is_bool($inner) => $inner ? '1' : '0',
                                default => $inner,
                            };
                        } else {

                            $escaped = array_map(
                                fn($v) => is_string($v) ? "'" . addslashes($v) . "'" : (is_null($v) ? "NULL" : $v),
                                $value
                            );
                            $result .= implode(', ', $escaped);
                        }
                        break;

                    case is_object($value):
                        $result .= method_exists($value, '__toString')
                            ? "'" . addslashes((string)$value) . "'"
                            : "'[object]'";
                        break;

                    default:
                        $result .= $value;
                }
            }
        }

        return $result;
    }

    // Security

    /**
     * Returns a whitelist of allowed SQL functions for use in expressions.
     *
     * This list is used to validate expressions passed to methods like groupBy() and orderBy(),
     * ensuring only safe, known SQL functions are allowed. Prevents abuse of dangerous or
     * unsupported SQL functions (e.g., SLEEP, LOAD_FILE, etc.).
     *
     * The function names are matched case-insensitively during validation.
     *
     * @return array List of allowed SQL function names (in uppercase).
     */
    protected function _secureAllowedFunctions(): array
    {
        return [
            'DATE', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
            'DATE_FORMAT', 'CONCAT', 'CONCAT_WS', 'LEFT', 'RIGHT', 'UPPER', 'LOWER',
            'LENGTH', 'TRIM', 'ROUND', 'ABS', 'IF', 'IFNULL', 'COALESCE',
            'SUBSTRING', 'SUBSTRING_INDEX', 'LOCATE', 'FIELD', 'GREATEST', 'LEAST',
            'LPAD', 'RPAD', 'MD5', 'CASE', 'CONVERT_TZ', 'JSON_UNQUOTE', 'JSON_EXTRACT'
        ];
    }

    /**
     * Escapes a table and column identifier for use in SQL queries.
     *
     * Converts dot notation (e.g. "table.column") into properly backtick-quoted form
     * suitable for MySQL (e.g. "`table`.`column`") to prevent syntax errors and
     * SQL injection via identifiers.
     *
     * @param string $column The column identifier, possibly with table prefix (dot notation).
     * @return string The escaped identifier with backticks.
     */
    protected function _secureEscapeTableAndColumn(string $column): string
    {
        try {
            $this->_secureValidateSqlExpression($column); // validiert – bei Erfolg: nix passiert
            return $column; // also hier sauber zurückgeben
        } catch (\Throwable) {
            return '`' . str_replace('.', '`.`', $column) . '`'; // fallback: escapen
        }
    }

    /**
     * Extracts individual expressions from a SQL CASE statement for further validation.
     *
     * This helper method is used to safely parse and dissect complex CASE expressions into
     * smaller parts such as conditions, THEN values, and the optional ELSE value.
     *
     * Example:
     *   CASE WHEN a = 1 THEN 'x' WHEN b = 2 THEN 'y' ELSE 'z' END
     *   → ['a = 1', "'x'", 'b = 2', "'y'", "'z'"]
     *
     * The returned array can then be passed recursively to the SQL expression validator.
     *
     * @param string $expr The full CASE expression to parse.
     * @return array Array of extracted expression parts.
     * @throws \InvalidArgumentException|\Throwable If the CASE expression is malformed.
     */
    protected function _secureExtractCaseParts(string $expr): array
    {
        $expr = preg_replace('/\s+/', ' ', trim($expr));
        $result = [];

        if (!preg_match_all('/WHEN (.+?) THEN (.+?)(?= WHEN| ELSE| END)/i', $expr, $matches, PREG_SET_ORDER)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Malformed CASE expression: {$expr}"),
                '_secureExtractCaseParts'
            );
        }

        foreach ($matches as $m) {
            $result[] = $m[1];
            $result[] = $m[2];
        }

        if (preg_match('/ELSE (.+?) END$/i', $expr, $elseMatch)) {
            $result[] = $elseMatch[1];
        }

        return $result;
    }

    protected function _secureGetBaseTableName(): ?string
    {
        $raw = $this->_tableName;

        if (preg_match('/^`?([a-zA-Z0-9_]+)`?(?:\s+[a-zA-Z0-9_]+)?$/', $raw, $m)) {
            // return str_replace($this->prefix, '', $m[1]);
            // fix - der Prefix ist ja Bestandteil des Tabellennamens - es sollen nur Aliase gelöscht werden
            return $m[1];
        }

        return null;
    }

    /**
     * Retrieves column metadata for a given column in the current base table.
     *
     * @param string $column
     * @return array{name: string, type: string, enumValues?: string[]}|null
     */
    protected function _secureGetColumnMeta(string $column): ?array
    {
        $table = $this->_secureGetBaseTableName();

        if (!$table || preg_match('/[^\w$]/', $table)) {
            return null;
        }

        // Cache hit
        if (isset($this->_columnCache[$table][$column])) {
            $entry = $this->_columnCache[$table][$column];
            return is_array($entry) && isset($entry['name'], $entry['type']) ? $entry : null;
        }

        try {
            $pdo = $this->connect($this->defConnectionName);
            $stmt = $pdo->query("DESCRIBE `{$table}`");
            $columns = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            foreach ($columns as $col) {
                $colName = $col['Field'];
                $colType = strtolower($col['Type']);

                $baseType = preg_split('/\s+/', preg_replace('/\(.*/', '', $colType))[0] ?? '';

                $entry = ['name' => $colName, 'type' => $baseType];

                if (str_starts_with($colType, 'enum')) {
                    preg_match_all("/'([^']+)'/", $colType, $matches);
                    $entry['enumValues'] = $matches[1] ?? [];
                }

                $this->_columnCache[$table][$colName] = $entry;
            }

            $entry = $this->_columnCache[$table][$column] ?? null;
            return is_array($entry) && isset($entry['name'], $entry['type']) ? $entry : null;

        } catch (\Throwable $e) {
            $this->logException($e, '_secureGetColumnMeta');
            return null;
        }
    }

    /**
     * Validates whether the given SQL operator is allowed and safe to use in WHERE clauses.
     *
     * Only a specific set of common and secure SQL comparison operators is permitted.
     * This helps prevent abuse of unsupported or dangerous operators (e.g. user-supplied "OR", "||", etc).
     *
     * @param string $op The SQL operator to check (e.g. '=', 'LIKE', 'IS NOT')
     * @return bool True if the operator is in the allowed list, false otherwise
     */
    protected function _secureIsAllowedOperator(string $op): bool
    {
        $allowed = [
            '=', '!=', '<>', '<', '<=', '>', '>=',
            'LIKE', 'NOT LIKE',
            'IN', 'NOT IN',
            'BETWEEN', 'NOT BETWEEN',
            'IS', 'IS NOT'
        ];

        return in_array(strtoupper(trim($op)), $allowed, true);
    }

    /**
     * Check whether a column name or SQL function expression is considered safe for use in WHERE, SELECT etc.
     *
     * This method validates that the column is either:
     *  1. A valid plain column name (including optional table alias and backticks)
     *  2. A safe SQL function call (e.g. COUNT(*), SUM(price), DATE(created_at))
     *
     * @param string $column The column name or SQL function to validate
     * @return bool True if safe, false if potentially dangerous or malformed
     */
    protected function _secureIsSafeColumn(string $column): bool
    {
        if (preg_match('/^`?[a-zA-Z_][a-zA-Z0-9_]*`?(\.`?[a-zA-Z_][a-zA-Z0-9_]*`?)?$/', $column)) {
            return true;
        }

        if (preg_match('/^([A-Z_]+)\(\s*(\*|`?[a-zA-Z_][a-zA-Z0-9_]*`?(\.`?[a-zA-Z_][a-zA-Z0-9_]*`?)?)\s*\)$/i', $column, $matches)) {
            $allowedFunctions = [
                'DATE', 'YEAR', 'MONTH', 'DAY', 'TIME', 'HOUR', 'MINUTE', 'SECOND',
                'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'
            ];

            $func = strtoupper($matches[1]);
            $arg  = $matches[2];

            return in_array($func, $allowedFunctions, true);
        }

        return false;
    }

    /**
     * Determines whether a given string is a quoted SQL string literal.
     *
     * Accepts both single-quoted ('example') and double-quoted ("example") formats.
     * This is primarily used during SQL expression validation to allow
     * static string values in GROUP BY or ORDER BY clauses.
     *
     * Note: Escaped quotes or complex nested quotes are not handled here –
     * this method is intentionally strict for security reasons.
     *
     * @param string $arg The string to check.
     * @return bool True if the argument is a simple quoted string, false otherwise.
     */
    protected function _secureIsQuotedString(string $arg): bool
    {
        return preg_match("/^'[^']*'$/", $arg) || preg_match('/^"[^"]*"$/', $arg);
    }

    /**
     * Checks if a value string contains suspicious SQL injection patterns.
     */
    protected function _secureLooksSuspicious(mixed $value): bool
    {

        if (!is_string($value)) {
            return false;
        }

        $value = strtolower(trim($value));
        $value = preg_replace('/\s+/', ' ', $value); // Normalisiert Whitespace zu einfachem Leerzeichen

        // Verdächtige SQL-Schlüsselwörter oder Zeichen
        $blacklist = [
            "'", '"', '\\',  // Quote & Escape Characters

            ';', '--', '/*', '*/',

            ' or ', ' and ', ' xor ',

            'sleep(', 'benchmark(', 'union ', 'select ', 'insert ', 'update ',
            'delete ', 'drop ', 'truncate ', 'exec(', 'execute ', 'version()',
            'char(', 'ascii(', 'hex(',
        ];

        foreach ($blacklist as $needle) {
            if (str_contains($value, $needle)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Validates all HAVING conditions for security and consistency.
     *
     * This is called during query preparation to ensure that the HAVING clause
     * does not contain any unsafe expressions, unvalidated subqueries, or dangerous placeholders.
     *
     * @param array $having The internal _pendingHaving array to validate.
     * @param array $allowedAliases List of allowed aliases (e.g. from SELECT or subqueries).
     *
     * @throws \InvalidArgumentException On any suspicious or unsupported input.
     * @throws \Throwable
     */
    protected function _secureSanitizeHaving(array $having, array $allowedAliases = []): void
    {
        foreach ($having as $index => $condition) {
            if (!is_array($condition)) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Invalid HAVING condition at index {$index} (not an array)"),
                    '_secureSanitizeHaving'
                );
            }

            if (count($condition) === 3) {
                [$column, $value, $operator] = $condition;
                $cond = 'AND';
            } elseif (count($condition) === 4) {
                [$cond, $column, $operator, $value] = $condition;
            } else {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Invalid HAVING condition at index {$index} (must have 3 or 4 elements)"),
                    '_secureSanitizeHaving'
                );
            }

            // 1. Ensure column is a valid string
            if (!is_string($column) || trim($column) === '') {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Missing or invalid column in HAVING at index {$index}"),
                    '_secureSanitizeHaving'
                );
            }

            // 2. Allow whitelisted aliases or check as regular column
            $isAlias = in_array($column, $allowedAliases, true);
            if (!$isAlias && !$this->_secureIsSafeColumn($column)) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Unsafe column in HAVING: {$column}"),
                    '_secureSanitizeHaving'
                );
            }

            // 3. Validate operator
            if (!$this->_secureIsAllowedOperator($operator)) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Invalid operator in HAVING: {$operator}"),
                    '_secureSanitizeHaving'
                );
            }

            // 4. Handle subquery
            if (is_object($value) && method_exists($value, 'getSubQuery')) {
                $sub = $value->getSubQuery();
                if (empty($sub['query']) || !is_string($sub['query'])) {
                    $this->reset(true);
                    throw $this->handleException(
                        new \InvalidArgumentException("Invalid subquery in HAVING at index {$index}"),
                        '_secureSanitizeHaving'
                    );
                }
            }

            // 5. Handle special placeholders
            elseif (is_array($value) && count($value) === 1 && is_string(key($value))) {
                $flag = key($value);
                $payload = current($value);

                switch ($flag) {
                    case '[F]':
                        $this->_secureValidateFunc($payload[0] ?? '');
                        break;

                    case '[I]':
                        foreach ((array)$payload as $id) {
                            if (!preg_match('/^[a-zA-Z0-9_\.]+$/', $id)) {
                                $this->reset(true);
                                throw $this->handleException(
                                    new \InvalidArgumentException("Invalid identifier in [I]: {$id}"),
                                    '_secureSanitizeHaving'
                                );
                            }
                        }
                        break;

                    case '[N]':
                        foreach ((array)$payload as $num) {
                            if (!is_int($num)) {
                                $this->reset(true);
                                throw $this->handleException(
                                    new \InvalidArgumentException("Non-integer value in [N]: " . json_encode($num)),
                                    '_secureSanitizeHaving'
                                );
                            }
                        }
                        break;

                    default:
                        $this->reset(true);
                        throw $this->handleException(
                            new \InvalidArgumentException("Unknown placeholder in HAVING: {$flag}"),
                            '_secureSanitizeHaving'
                        );

                }
            }

            // 6. Arrays for BETWEEN / IN
            elseif (in_array(strtoupper($operator), ['BETWEEN', 'NOT BETWEEN'], true)) {
                if (!is_array($value) || count($value) !== 2) {
                    $this->reset(true);
                    throw $this->handleException(
                        new \InvalidArgumentException("HAVING BETWEEN requires array with exactly 2 values."),
                        '_secureSanitizeHaving'
                    );
                }
                foreach ($value as $v) {
                    if (!is_scalar($v)) {
                        $this->reset(true);
                        throw $this->handleException(
                            new \InvalidArgumentException("Non-scalar value in HAVING BETWEEN array."),
                            '_secureSanitizeHaving'
                        );
                    }
                }
            }

            elseif (in_array(strtoupper($operator), ['IN', 'NOT IN'], true)) {
                if (!is_array($value) || empty($value)) {
                    $this->reset(true);
                    throw $this->handleException(
                        new \InvalidArgumentException("HAVING IN requires non-empty array."),
                        '_secureSanitizeHaving'
                    );
                }
                foreach ($value as $v) {
                    if (!is_scalar($v)) {
                        $this->reset(true);
                        throw $this->handleException(
                            new \InvalidArgumentException("Non-scalar value in HAVING IN array."),
                            '_secureSanitizeHaving'
                        );

                    }
                }
            }

            // 7. Scalar with suspicious content
            elseif (is_scalar($value) && $this->_secureLooksSuspicious($value)) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Suspicious value in HAVING: " . json_encode($value)),
                    '_secureSanitizeHaving'
                );
            }

            // 8. Unsupported type
            elseif (!is_scalar($value) && !is_null($value)) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Unsupported value type in HAVING: " . gettype($value)),
                    '_secureSanitizeHaving'
                );
            }

            // Store the validated condition
            $this->_having[] = [$cond, $column, $operator, $value];
        }
    }

    /**
     * Performs a basic security check on an SQL expression to detect dangerous keywords or patterns.
     *
     * This method scans the expression for forbidden SQL constructs such as:
     * - Statement terminators (`;`)
     * - Comment indicators (`--`, `#`, `/*`)
     * - Dangerous SQL keywords (`DROP`, `UNION`, `SLEEP`, `LOAD_FILE`, etc.)
     *
     * If any pattern matches, an InvalidArgumentException is thrown immediately.
     * This method is designed to be a first-level defense against SQL injection.
     *
     * @param string $expr The raw SQL expression to validate.
     * @throws \InvalidArgumentException If a forbidden keyword or pattern is found.
     * @throws \Throwable
     */
    protected function _secureSanitizeSqlExpr(string $expr): void
    {
        $forbidden = [
            '/;/', '/--/', '/#/', '/\/\*/',
            '/\b(UNION|DROP|DELETE|UPDATE|INSERT|REPLACE|SLEEP|BENCHMARK|LOAD_FILE|OUTFILE)\b/i'
        ];

        foreach ($forbidden as $pattern) {
            if (preg_match($pattern, $expr)) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Illegal keyword in SQL expression: {$expr}"),
                    '_secureSanitizeSqlExpr'
                );
            }
        }
    }

    /**
     * Validates and transforms deferred WHERE conditions based on column metadata.
     *
     * This method checks each deferred condition (added via secureWhere)
     * against the actual column types of the target table. If a value
     * doesn't match the expected data type, it throws an exception
     * and resets the internal query state.
     *
     * @param array $stack Reference to the internal _where stack.
     *                     Each item is either a raw condition or a __DEFERRED__ entry with metadata.
     *
     * @throws \RuntimeException If no valid table is defined.
     * @throws \InvalidArgumentException If a suspicious or invalid value is detected.
     * @throws \Throwable
     */
    protected function _secureSanitizeWhere(array &$stack): void
    {
        $tableRaw = $this->_tableName ?? '';
        $table = preg_split('/\s+/', (string)$tableRaw)[0] ?? '';
        $table = str_replace('`', '', $table);

        if (!$table) {
            $this->reset(true);
            throw $this->handleException(
                new \RuntimeException("Cannot resolve deferred WHERE: table name missing"),
                '_secureSanitizeWhere'
            );
        }

        foreach ($stack as $i => $item) {
            if (!is_array($item) || !isset($item['__DEFERRED__'])) {
                $cond     = $item[0] ?? 'AND';
                $column   = $item[1] ?? null;
                $operator = $item[2] ?? '=';
                $value    = $item[3] ?? null;

                $stack[$i] = [$cond, $column, $operator, $value];
                continue;
            }

            // DEFERRED
            $column   = $item['column'];
            $value    = $item['value'];
            $operator = $item['operator'] ?? '=';
            $cond     = $item['cond'] ?? 'AND';

            $meta = $this->_secureGetColumnMeta($column, $table);
            $type = strtolower($meta['type'] ?? '');
            $allowed = true;

            if (in_array($type, ['tinyint', 'smallint', 'mediumint', 'int', 'bigint'], true)) {
                $tmp = (int) $value;
                $allowed = (strlen((string)$tmp) === strlen((string)$value));
            }
            elseif (in_array($type, ['float', 'double', 'decimal', 'real'], true)) {
                $allowed = is_numeric($value);
            }
            elseif (isset($meta['enumValues']) && is_array($meta['enumValues'])) {
                $allowed = in_array((string) $value, $meta['enumValues'], true);
            }

            if (!$allowed) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Suspicious value in deferred WHERE: {$column} = {$value}"),
                    '_secureSanitizeWhere'
                );
            }

            $stack[$i] = [$cond, $column, $operator, $value];
        }
    }

    /**
     * Splits a SQL function argument list into individual arguments.
     *
     * This method is used to safely parse argument strings from SQL function calls
     * (e.g., "CONCAT(a, 'b,c', IF(x, y, z))") by considering nesting and quoted strings.
     * It correctly handles:
     * - Quoted strings (single or double quotes)
     * - Nested parentheses (e.g., nested function calls)
     * - Commas inside quotes or nested calls (which should not split)
     *
     * Example:
     *   Input: "a, 'b,c', IF(x, y, z)"
     *   Output: ["a", "'b,c'", "IF(x, y, z)"]
     *
     * @param string $args Raw function argument string.
     * @return array List of individual argument strings.
     */
    protected function _secureSplitFunctionArgs(string $args): array
    {
        $result = [];
        $depth = 0;
        $buffer = '';
        $inQuote = false;
        $quoteChar = '';
        $len = strlen($args);

        for ($i = 0; $i < $len; $i++) {
            $char = $args[$i];

            if (($char === "'" || $char === '"')) {
                if ($inQuote && $char === $quoteChar) {
                    $inQuote = false;
                } elseif (!$inQuote) {
                    $inQuote = true;
                    $quoteChar = $char;
                }
            } elseif (!$inQuote) {
                if ($char === '(') {
                    $depth++;
                } elseif ($char === ')') {
                    $depth--;
                } elseif ($char === ',' && $depth === 0) {
                    $result[] = trim($buffer);
                    $buffer = '';
                    continue;
                }
            }

            $buffer .= $char;
        }

        if (trim($buffer) !== '') {
            $result[] = trim($buffer);
        }

        return $result;
    }

    /**
     * Validates a date string or timestamp and returns a normalized 'Y-m-d H:i:s' format.
     * Triggers reset(true) on invalid values.
     *
     * @param mixed $input Date string or 10-digit timestamp.
     * @return string      Validated and normalized date string.
     * @throws \InvalidArgumentException
     * @throws \Throwable
     */
    protected function _secureValidateAndFormatDate(mixed $input): string
    {
        if (!is_scalar($input)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Non-scalar date value."),
                '_secureValidateAndFormatDate'
            );
        }

        $str = (string) $input;

        // Convert timestamp to datetime
        if (preg_match('/^\d{10}$/', $str)) {
            $str = date('Y-m-d H:i:s', (int)$str);
        }

        foreach (['Y-m-d', 'Y-m-d H:i:s'] as $format) {
            $dt = \DateTime::createFromFormat($format, $str);
            if ($dt && $dt->format($format) === $str) {
                return $str;
            }
        }

        $this->reset(true);
        throw $this->handleException(
            new \InvalidArgumentException("Invalid date or datetime: '{$str}'"),
            '_secureValidateAndFormatDate'
        );
    }

    /**
     * Validates insert/update/replace values for correctness and safety.
     *
     * Allowed:
     * - Scalar values (int, string, float, bool, null)
     * - Special array values: ['[F]'=>..., '[I]'=>..., '[N]'=>...]
     * - Subquery objects with getSubQuery()
     *
     * @param array $insertData Associative array of column => value
     * @param string $context   Internal context like 'insert' or 'replace' for error messages
     * @return array            Validated and potentially transformed values
     * @throws \Exception|\Throwable       On invalid structure or unsafe values
     */
    protected function _secureValidateInsertValues(array $insertData, string $context = 'insert'): array
    {
        $validated = [];

        foreach ($insertData as $col => $val) {
            if (!$this->_secureIsSafeColumn($col)) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Unsafe column name '{$col}' in {$context}()"),
                    $context
                );
            }

            // Subquery object?
            if (is_object($val) && method_exists($val, 'getSubQuery')) {
                $validated[$col] = $val;
                continue;
            }

            // Special values like [F], [I], [N]
            if (is_array($val) && count($val) === 1) {
                $key = array_key_first($val);

                if (in_array($key, ['[F]', '[I]', '[N]'], true)) {
                    $inner = $val[$key];

                    if (is_array($inner)) {
                        foreach ($inner as $entry) {
                            if (!is_string($entry) || !preg_match('/^[A-Z0-9_ ()+\-*\/.]+$/i', $entry)) {
                                $this->reset(true);
                                throw $this->handleException(
                                    new \Exception("Invalid value in '{$key}' for column '{$col}' in {$context}()"),
                                    $context
                                );
                            }
                        }
                    } elseif (!is_string($inner) || !preg_match('/^[A-Z0-9_ ()+\-*\/.]+$/i', $inner)) {
                        $this->reset(true);
                        throw $this->handleException(
                            new \Exception("Invalid value in '{$key}' for column '{$col}' in {$context}()"),
                            $context
                        );
                    }

                    $validated[$col] = [$key => $inner];
                    continue;
                }
            }

            // Allow only scalar values
            if (!is_scalar($val) && $val !== null) {
                $this->reset(true);
                throw $this->handleException(
                    new \Exception("Invalid non-scalar value for column '{$col}' in {$context}()"),
                    $context
                );
            }

            $validated[$col] = $val;
        }

        return $validated;
    }

    /**
     * Validates a table name to ensure it only contains allowed characters.
     *
     * @param string $tableName The table name to validate.
     * @return string The validated table name (can apply trim or other cleanups).
     * @throws \InvalidArgumentException If the table name is invalid.
     * @throws \Throwable
     */
    protected function _secureValidateTable(string $tableName): string
    {
        $tableName = trim($tableName, "`'\"");

        if (str_starts_with($tableName, '(')) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Raw subqueries as table names are not allowed. Use subQuery() instead."),
                '_secureValidateTable'
            );
        }

        $parts = preg_split('/\s+/', $tableName);
        $name  = $parts[0];
        $alias = $parts[1] ?? null;

        if (!preg_match('/^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+){0,2}$/', $name)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid characters found in table name: '{$name}'."),
                '_secureValidateTable'
            );
        }

        if ($alias !== null && !preg_match('/^[a-zA-Z0-9_]+$/', $alias)) {
            $this->reset(true);
            throw $this->handleException(
                new \InvalidArgumentException("Invalid characters found in alias: '{$alias}'."),
                '_secureValidateTable'
            );
        }

        return $alias ? "{$name} {$alias}" : $name;
    }

    /**
     * @throws \Throwable
     */
    protected function _secureValidateFunc(string $expr): string
    {
        $expr = trim($expr);

        // Whitelist für explizite Einzel-Funktionen
        $allowed = [
            'NOW()',
            'UUID()',
            'CURRENT_TIMESTAMP',
            'CURDATE()',
            'CURTIME()'
        ];

        if (in_array(strtoupper($expr), $allowed, true)) {
            return $expr;
        }

        // INTERVAL-Ausdrücke wie: "updated_at + interval 2 day"
        if (
            preg_match(
                '/^[a-zA-Z0-9_]+\(\)?\s*(\+|\-)\s*interval\s*\d+\s+(SECOND|MINUTE|HOUR|DAY|MONTH|YEAR)$/i',
                $expr
            ) ||
            preg_match(
                '/^[a-zA-Z0-9_]+\s*(\+|\-)\s*interval\s*\d+\s+(SECOND|MINUTE|HOUR|DAY|MONTH|YEAR)$/i',
                $expr
            )
        ) {
            return $expr;
        }

        $this->reset(true);
        throw $this->handleException(
            new \InvalidArgumentException("Unsafe [F] expression: {$expr}"),
            '_secureValidateFunc'
        );
    }

    /**
     * Validates a SQL expression to ensure it is safe and syntactically correct.
     *
     * This method is used to prevent SQL injection and enforce strict safety rules
     * when allowing user-defined expressions in GROUP BY or ORDER BY clauses.
     *
     * Supported formats:
     * - Simple column names (e.g. `status`, `users.name`)
     * - Quoted string or numeric literals
     * - Allowed SQL functions with validated arguments (e.g. ROUND(price, 2))
     * - Safe CASE ... WHEN ... THEN ... ELSE ... END expressions
     * - Safe binary expressions (e.g. total > 100)
     *
     * The method supports recursive parsing of nested functions and expressions.
     * Any use of dangerous keywords or disallowed constructs will result in an exception.
     *
     * @param string $expr The SQL expression to validate.
     * @throws \InvalidArgumentException If the expression is unsafe or malformed.
     * @throws \Throwable
     */
    protected function _secureValidateSqlExpression(string $expr): void
    {
        $expr = trim($expr);
        $this->_secureSanitizeSqlExpr($expr);

        if (preg_match('/^[a-zA-Z0-9_.-]+$/', $expr)) {
            return;
        }

        if (is_numeric($expr) || $this->_secureIsQuotedString($expr)) {
            return;
        }

        if (stripos($expr, 'CASE') === 0) {

            $parts = $this->_secureExtractCaseParts($expr);
            foreach ($parts as $partExpr) {
                $this->_secureValidateSqlExpression($partExpr);
            }
            return;
        }

        if (preg_match('/^([a-zA-Z0-9_]+)\s*\((.*)\)$/s', $expr, $match)) {
            $function = strtoupper($match[1]);
            $args = $this->_secureSplitFunctionArgs($match[2]);

            if (!in_array($function, $this->_secureAllowedFunctions(), true)) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException("Function not allowed: {$function}"),
                    '_secureValidateSqlExpression'
                );
            }

            foreach ($args as $arg) {
                $this->_secureValidateSqlExpression($arg);
            }

            return;
        }

        if (preg_match('/^[\w\.\'"]+\s+IS(\s+NOT)?\s+NULL$/i', $expr)) {
            return;
        }

        if (preg_match('/^[\w\.\'"]+\s*(=|<>|!=|<|>|<=|>=)\s*[\w\.\'"]+$/', $expr)) {
            return;
        }
        $this->reset(true);
        throw $this->handleException(
            new \InvalidArgumentException("Invalid SQL expression: {$expr}"),
            '_secureValidateSqlExpression'
        );
    }

    /**
     * @throws \Throwable
     */
    protected function _secureValidateSpecialValue(mixed $value): void
    {
        // SubQuery
        if ($value instanceof self && $value->isSubQuery === true) {
            $sql = (string)$value;
            $this->_secureSanitizeSqlExpr($sql); // Grundprüfung auf gefährliche Ausdrücke
            return;
        }

        // Platzhalter: [F], [I], [N]
        if (is_array($value)) {
            foreach ($value as $key => $val) {
                switch ($key) {
                    case '[F]':
                        if (!is_array($val) || !isset($val[0])) {
                            $this->reset(true);
                            throw $this->handleException(
                                new \InvalidArgumentException("Invalid [F] function format."),
                                '_secureValidateSpecialValue'
                            );
                        }
                        $this->_secureValidateSqlExpression($val[0]);
                        break;

                    case '[I]':
                        if (!is_string($val) || !preg_match('/^\s*[\+\-]\s*\d+(\.\d+)?\s*$/', $val)) {
                            $this->reset(true);
                            throw $this->handleException(
                                new \InvalidArgumentException("Invalid [I] increment format."),
                                '_secureValidateSpecialValue'
                            );
                        }
                        break;

                    case '[N]':
                        if ($val !== '' && !$this->_secureIsSafeColumn($val)) {
                            $this->reset(true);
                            throw $this->handleException(
                                new \InvalidArgumentException("Invalid column name in [N]."),
                                '_secureValidateSpecialValue'
                            );
                        }
                        break;

                    default:
                        $this->reset(true);
                        throw $this->handleException(
                            new \InvalidArgumentException("Unknown special SQL placeholder: {$key}"),
                            '_secureValidateSpecialValue'
                        );
                }
            }
        }
    }

    /**
     * Hook for saving debug info before internal state is cleared.
     */
    protected function _beforeReset(): void
    {
        if (!empty($this->_query) && !empty($this->_bindParams)) {
            $this->_lastQuery = $this->_replacePlaceHolders($this->_query, $this->_bindParams);
        }
    }

    /**
     * Resets all internal query-related state after a statement is executed.
     *
     * This includes WHERE, JOIN, GROUP BY, ORDER BY, bound parameters, query string,
     * and other modifiers. It also records query trace information if enabled.
     *
     * @return self Fluent interface for method chaining
     */
    protected function reset(bool $hard = false): self
    {
        $this->_lastDebugQuery = '';

        if (!empty($this->_query)) {
            $this->_lastDebugQuery = $this->_replacePlaceHolders($this->_query, $this->_bindParams ?? []);
        } else {
            $this->_lastDebugQuery = '-- Query not available: possibly blocked by exception or security rule';

            if (!empty($this->_stmtError)) {
                $code = $this->_stmtErrno ?? '???';
                $this->_lastDebugQuery .= " [Error #{$code}: {$this->_stmtError}]";
            }
        }

        if ($this->traceEnabled) {
            $this->trace[] = [
                $this->_lastQuery,
                (microtime(true) - $this->traceStartQ),
                $this->_traceGetCaller()
            ];
        }

        if ($hard) {
            $this->_beforeReset();
            $this->_pendingHaving = [];
            $this->_selectAliases = [];
            $this->_returnKey[$this->defConnectionName] = null;
            $this->_returnType[$this->defConnectionName] = 'array';
            $this->_oldPlWasSet[$this->defConnectionName] = false;
            $this->_newPlWasSet[$this->defConnectionName] = false;
            $this->_manualErrorSet[$this->defConnectionName] = false;
            $this->_batchMode[$this->defConnectionName] = false;
        }

        $this->_where = [];
        $this->_having = [];
        $this->_join = [];
        $this->_joinAnd = [];
        $this->_orderBy = [];
        $this->_groupBy = [];
        $this->_bindParams = [];
        $this->_pendingJoins = [];
        $this->_joinWheres = [];
        $this->_joinAliases = [];
        $this->_query = null;
        $this->_queryOptions = [];
        $this->_nestJoin = false;
        $this->_forUpdate = false;
        $this->_lockInShareMode = false;
        $this->_tableName = '';
        $this->_updateColumns = null;
        $this->_tableAlias = null;
        $this->_stmtError = null;
        $this->_stmtErrno = null;
        $this->autoReconnectCount = 0;
        $this->_checkBulkInput[$this->defConnectionName] = false;

        return $this;
    }

    // ERROR HANDLING

    /**
     * Internal helper to determine which function and file initiated the last query.
     *
     * Used for debugging/tracing query origin (if tracing is enabled).
     *
     * @return string Formatted trace string: function, file path, and line number
     */
    private function _traceGetCaller(): string
    {
        $dd = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
        $caller = next($dd);

        while (isset($caller['file']) && $caller['file'] === __FILE__) {
            $caller = next($dd);
        }

        $function = $caller['function'] ?? 'unknown';
        $file     = $caller['file'] ?? 'unknown';
        $line     = $caller['line'] ?? 0;

        return static::class . "->" . $function .
            "() >>  file \"" . str_replace($this->traceStripPrefix, '', $file) . "\" line #" . $line;
    }

    /**
     * Internal notice logger (always logs).
     *
     * Used to log soft warnings like deprecated access
     * to $pageLimit or fallback behavior.
     *
     * @param string $message
     */
    protected function logNotice(string $message): void
    {
        error_log("[Notice] $message");
    }

    protected function logException(\Throwable $e, ?string $context = null): void
    {
        if (($this->_debugLevel[$this->defConnectionName] ?? 0) < 1) {
            return;
        }

        $message = "[Exception]";
        if ($context) {
            $message .= " [$context]";
        }
        $message .= " " . $e->getMessage();

        error_log($message);

    }

    protected function logQuery(string $sql, ?array $bindings = null): void
    {
        if (($this->_debugLevel[$this->defConnectionName] ?? 0) < 2) {
            return;
        }

        error_log("[Query] " . $sql);

        if (!empty($bindings)) {
            error_log("[Bindings] " . print_r($bindings, true));
        }

    }

    protected function countQuery(): void
    {
        $conn = $this->defConnectionName;

        if (
            !($this->_manualErrorSet[$conn] ?? false) &&
            !($this->_batchMode[$conn] ?? false)
        ) {
            if (!isset($this->_queryCounter[$conn])) {
                $this->_queryCounter[$conn] = 0;
            }
            $this->_queryCounter[$conn]++;
        }
    }

    protected function setManualLastErrors(): void
    {
        $this->_manualErrorSet[$this->defConnectionName] = true;
        if (!isset($this->_queryCounter[$this->defConnectionName])) {
            $this->_queryCounter[$this->defConnectionName] = 0;
        }
        $this->_queryCounter[$this->defConnectionName]++;

        $this->_errorStore[$this->defConnectionName] = [
            'error' => $this->_stmtError,
            'code'  => $this->_stmtErrno,
            'count' => $this->_queryCounter[$this->defConnectionName],
        ];

        $this->_stmtError = null;
        $this->_stmtErrno = null;

        // $this->_cleanupOldErrors();
    }

    protected function _cleanupOldErrors(): void
    {
        $conn = $this->defConnectionName;
        $current = $this->_queryCounter[$conn] ?? 0;

        foreach ($this->_errorStore as $c => $entry) {
            if ($c === $conn && is_array($entry)) {
                if (($entry['count'] ?? 0) < $current) {
                    unset($this->_errorStore[$c]);
                }
            }
        }
    }

    /**
     * Handles and logs an exception and rethrows it.
     *
     * @throws \Throwable
     */
    protected function handleException(\Throwable $e, string $context = ''): mixed
    {

        $prefix = '[Exception]';
        if ($context) {
            $prefix .= " [$context]";
        }

        if (!isset($this->_queryCounter[$this->defConnectionName])) {
            $this->_queryCounter[$this->defConnectionName] = 0;
        }
        $this->_queryCounter[$this->defConnectionName]++;

        $this->_errorStore[$this->defConnectionName] = [
            'error' => $prefix . ' ' . $e->getMessage(),
            'code'  => $e->getCode(),
            'count' => $this->_queryCounter[$this->defConnectionName],
        ];



        if (($this->_debugLevel[$this->defConnectionName] ?? 0) >= 2 && !empty($this->_query)) {
            $this->logQuery($this->_query, $this->_bindParams ?? null);
        }

        $this->logException($e, $context);
        $this->_cleanupOldErrors();
        throw $e;
    }

    /**
     * @throws \Throwable
     */
    public function __toString(): string
    {
        if (!$this->isSubQuery) {
            $this->reset(true);
            throw $this->handleException(
                new \RuntimeException('__toString() is only allowed on subQuery objects.'),
                '__toString'
            );
        }

        $this->_buildQuery();
        return '(' . $this->_query . ')';
    }

    /**
     * @throws \Throwable
     */
    public function __set(string $name, mixed $value): void
    {
        if ($name === 'pageLimit') {

            if (!is_numeric($value) || (int) $value < 1) {
                $this->reset(true);
                throw $this->handleException(
                    new \InvalidArgumentException(
                        "Invalid value for pageLimit: " . var_export($value, true) . ". Must be a positive integer. "
                        . "Use setPageLimit() instead."
                    ),
                    '__set'
                );
            }

            $intValue = (int) $value;

            $this->logNotice(
                "Deprecated: \$pageLimit was set directly to $intValue on instance '{$this->defConnectionName}'. "
                . "Use setPageLimit() instead. This compatibility layer will be removed in v1.5.x"
            );

            $this->_pageLimit[$this->defConnectionName] = $intValue;
            $this->_oldPlWasSet[$this->defConnectionName] = true;

            return;
        }

        $this->reset(true);
        throw $this->handleException(
            new \RuntimeException("Cannot set undefined property: $name"),
            '__set'
        );
    }

    /**
     * Magic getter to support deprecated public access like $db->count.
     * Triggers a notice but still returns the correct value.
     *
     * @param string $name
     * @return mixed|null
     */
    public function __get(string $name): mixed
    {
        switch ($name) {
            case 'count':
                $this->logNotice("Deprecated: Accessing \$count directly on instance '{$this->defConnectionName}' is discouraged. Use getRowCount() instead. This will be removed in v1.5.x.");
                return $this->getRowCount();

            case 'totalCount':
                $this->logNotice("Deprecated: Accessing \$totalCount directly on instance '{$this->defConnectionName}' is discouraged. Use getTotalCount() instead. This will be removed in v1.5.x.");
                return $this->getTotalCount();

            case 'totalPages':
                $this->logNotice("Deprecated: Accessing \$totalPages directly on instance '{$this->defConnectionName}' is discouraged. Use getTotalPages() instead. This will be removed in v1.5.x.");
                return $this->getTotalPages();

            default:
                return null;
        }
    }
    public function getLastDebugQuery(): string
    {
        return $this->_lastDebugQuery;
    }

    public function clearLastQuery(): void
    {
        $this->_lastQuery        = '';
        $this->_lastDebugQuery   = '';
    }
}