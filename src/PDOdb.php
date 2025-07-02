<?php
/**
 * PDOdb Class – ThingEngineer compatible PDO wrapper
 *
 * @category  Database Access
 * @package   decMuc\PDOdb
 * @author    Lucky Fischer
 * @copyright Copyright (c) 2025 Lucky Fischer
 * @license   https://opensource.org/licenses/MIT MIT License
 * @link      https://github.com/decMuc/PDOdb
 * @version   1.0.2
 * @inspired-by https://github.com/ThingEngineer/PHP-MySQLi-Database-Class
 */


namespace decMuc\PDOdb;

final class PDOdb
{
    /**
     * Static instance for singleton usage.
     *
     * @var self
     */
    protected static $_instance;
    private string $prefix = '';

    /**
     * Table prefix for all queries.
     *
     * @var string
     */
    // protected string $prefix = '';

    /**
     * PDO connection instances, indexed by connection name.
     *
     * @var \PDO[]
     */
    protected array $_pdo = [];

    /**
     * The SQL query that is currently being prepared/executed.
     *
     * @var string|null
     */
    protected ?string $_query = null;

    /**
     * The last executed SQL query string (with values replaced).
     *
     * @var string|null
     */
    protected ?string $_lastQuery = null;

    /**
     * Query options for SELECT, INSERT, UPDATE, DELETE (e.g., SQL_CALC_FOUND_ROWS).
     *
     * @var array
     */
    protected array $_queryOptions = [];

    /**
     * Array holding join definitions for the current query.
     *
     * @var array
     */
    protected array $_join = [];

    /**
     * Array holding WHERE conditions for the current query.
     *
     * @var array
     */
    protected array $_where = [];

    /**
     * Array holding additional AND conditions for joined tables.
     *
     * @var array
     */
    protected array $_joinAnd = [];

    protected array $_pendingJoins = [];
    /**
     * Array holding HAVING conditions for the current query.
     *
     * @var array
     */
    protected array $_having = [];

    /**
     * Array holding ORDER BY definitions.
     *
     * @var array
     */
    protected array $_orderBy = [];

    /**
     * Array holding GROUP BY definitions.
     *
     * @var array
     */
    protected array $_groupBy = [];

    /**
     * Array holding locked tables during table-level locking.
     *
     * @var array
     */
    protected array $_tableLocks = [];

    /**
     * Current table lock method ("READ" or "WRITE").
     *
     * @var string
     */
    protected string $_tableLockMethod = "READ";

    /**
     * Array of parameters and types for the next prepared statement.
     *
     * @var array
     */
    // protected array $_bindParams = [''];
    protected array $_bindParams = [];
    /**
     * Number of rows returned/affected by the last get/getOne/select.
     *
     * @var int
     */
    public int $count = 0;

    /**
     * Total number of rows matching the last query with SQL_CALC_FOUND_ROWS.
     *
     * @var int
     */
    public int $totalCount = 0;

    /**
     * Last statement error message (from PDOStatement).
     *
     * @var string|null
     */
    protected ?string $_stmtError = null;


    /**
     * Last statement error code (from PDOStatement).
     *
     * @var string|null
     */
    protected ?string $_stmtErrno = null;

    /**
     * True if this object is being used as a subquery builder.
     *
     * @var bool
     */
    protected bool $isSubQuery = false;

    /**
     * Name or index of the last auto-incremented column.
     *
     * @var string|int|null
     */
    protected $_lastInsertId = null;


    protected ?string $_returnKey = null;
    /**
     * Columns for ON DUPLICATE KEY UPDATE operations.
     *
     * @var array|null
     */
    protected ?array $_updateColumns = null;

    /**
     * Holds the key name for mapping result arrays.
     * If set, results will be mapped as [$keyValue => $row, ...]
     * @var string|null
     */
    protected ?string $returnKey = null;

    /**
     * If true, join() results will be nested by table name.
     *
     * @var bool
     */
    protected bool $_nestJoin = false;

    /**
     * The table name (with prefix applied).
     *
     * @var string
     */
    private string $_tableName = '';

    protected ?string $_tableAlias = null;
    /**
     * If true, add FOR UPDATE to the query.
     *
     * @var bool
     */
    protected bool $_forUpdate = false;

    /**
     * If true, add LOCK IN SHARE MODE to the query.
     *
     * @var bool
     */
    protected bool $_lockInShareMode = false;

    /**
     * Key field for use with map() result sets.
     *
     * @var string|null
     */
    protected ?string $_mapKey = null;

    /**
     * Microtime of query execution start (for tracing).
     *
     * @var float
     */
    protected float $traceStartQ = 0.0;

    /**
     * Enable execution time tracing.
     *
     * @var bool
     */
    protected bool $traceEnabled = false;

    /**
     * Optional path prefix to strip from trace logs.
     *
     * @var string
     */
    protected string $traceStripPrefix = '';

    /**
     * Query execution trace log (array of [query, duration, caller]).
     *
     * @var array
     */
    public array $trace = [];

    /**
     * Rows per page for pagination.
     *
     * @var int
     */
    public int $pageLimit = 20;

    /**
     * Total page count for the last paginate() query.
     *
     * @var int
     */
    public int $totalPages = 0;

    /**
     * Connection settings for all configured PDO profiles.
     *
     * @var array
     */
    protected array $connectionsSettings = [];

    /**
     * Name of the default connection profile.
     *
     * @var string
     */
    public string $defConnectionName = 'default';

    /**
     * Enable automatic reconnect on lost connections.
     *
     * @var bool
     */
    public bool $autoReconnect = true;

    /**
     * Number of auto-reconnect attempts.
     *
     * @var int
     */
    protected int $autoReconnectCount = 0;

    /**
     * Indicates whether a transaction is currently in progress.
     *
     * @var bool
     */
    protected bool $_transaction_in_progress = false;

    /**
     * 'array', 'object' oder 'both'
     */
    protected string $_returnType = 'array';

    protected array $_joinWheres = [];

    protected int $debugLevel = 0;
    public function __construct(
        $host = null,
        $username = null,
        $password = null,
        $db = null,
        $port = null,
        $charset = 'utf8mb4',
        $socket = null
    ) {
        $prefix = '';

        if (defined('DEBUG_MODE') && DEBUG_MODE === true) {
            $this->debug(1);
        }

        // Support passing in PDO directly
        if ($host instanceof \PDO) {
            $this->pdoConnections['default'] = $host;
            return;
        }

        // Support array-based initialization
        if (is_array($host)) {
            foreach ($host as $key => $val) {
                switch ($key) {
                    case 'host':      $host = $val; break;
                    case 'username':  $username = $val; break;
                    case 'password':  $password = $val; break;
                    case 'db':        $db = $val; break;
                    case 'port':      $port = $val; break;
                    case 'socket':    $socket = $val; break;
                    case 'charset':   $charset = $val; break;
                    case 'prefix':    $prefix = $val; break;
                }
            }
        }

        $this->setPrefix($prefix);

        $this->addConnection('default', [
            'host'     => $host,
            'username' => $username,
            'password' => $password,
            'db'       => $db,
            'port'     => $port,
            'socket'   => $socket,
            'charset'  => $charset
        ]);

        self::$_instance = $this;
    }

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
                ]
            );
        }

        return $this->_pdo[$connectionName];
    }

    /**
     * Sets the default connection profile to be used for all upcoming operations.
     *
     * The given name must have been previously registered via addConnection().
     * This does not trigger an immediate connection attempt.
     *
     * @param string $name Name of the connection profile (as passed to addConnection)
     * @return static
     * @throws \InvalidArgumentException If the named connection profile is not registered
     */

    /* need a clone to handle slave connections */
    public function connection(string $name): static
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
     * @return static
     */
    public function addConnection(string $name, array $params): static
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
     * Checks whether this instance represents a subquery context.
     *
     * @return bool True if this is a subquery, false otherwise
     */
    public function isSubQuery(): bool
    {
        return $this->isSubQuery;
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

    /**
     * Returns the most recently created instance of the database handler.
     *
     * This allows access to the current DB context from static scope
     * after it was initialized somewhere else.
     *
     * @return static|null The last initialized instance or null if none exists
     */
    public static function getInstance(): ?static
    {
        return self::$_instance;
    }

    /**
     * Resets all internal query-related state after a statement is executed.
     *
     * This includes WHERE, JOIN, GROUP BY, ORDER BY, bound parameters, query string,
     * and other modifiers. It also records query trace information if enabled.
     *
     * @return static Fluent interface for method chaining
     */
    protected function reset(): static
    {
        if ($this->traceEnabled) {
            $this->trace[] = [
                $this->_lastQuery,
                (microtime(true) - $this->traceStartQ),
                $this->_traceGetCaller()
            ];
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
        $this->_query = null;
        $this->_queryOptions = [];
        $this->_returnType = 'array';
        $this->_nestJoin = false;
        $this->_forUpdate = false;
        $this->_lockInShareMode = false;
        $this->_tableName = '';
        $this->_lastInsertId = null;
        $this->_updateColumns = null;
        $this->_mapKey = null;

        // WICHTIG – ergänzt:
        $this->_tableAlias = null;
        $this->_stmtError = null;
        $this->_stmtErrno = null;
        $this->totalCount = 0;
        $this->count = 0;
        $this->_lastQuery = '';

        if (!$this->_transaction_in_progress) {
            $this->defConnectionName = 'default';
        }

        $this->autoReconnectCount = 0;

        return $this;
    }

    /**
     * Sets the return type to JSON for subsequent queries.
     *
     * @return static Returns the current instance for chaining.
     */
    public function jsonBuilder(): static
    {
        $this->_returnType = 'json';
        return $this;
    }

    /**
     * Sets the return type to array for subsequent queries.
     *
     * @return static
     */
    public function arrayBuilder(): static
    {
        $this->_returnType = 'array';
        return $this;
    }

    /**
     * Sets the return type to object for subsequent queries.
     *
     * @return static
     */
    public function objectBuilder(): static
    {
        $this->_returnType = 'object';
        return $this;
    }

    /**
     * Sets the table prefix used in all queries.
     *
     * This allows dynamic adjustment of the prefix at runtime,
     * useful for multi-tenant setups or prefixed table schemas.
     *
     * @param string $prefix The prefix to prepend to table names
     * @return static Fluent interface for method chaining
     */
    public function setPrefix(string $prefix): void
    {
        if (!preg_match('/^[a-zA-Z0-9_]*$/', $prefix)) {
            throw new \InvalidArgumentException("Invalid prefix: '{$prefix}'. Only letters, numbers and underscores are allowed.");
        }

        $this->prefix = $prefix;
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
        return match ($this->_returnType) {
            'array', 'json' => \PDO::FETCH_ASSOC,
            'object'        => \PDO::FETCH_OBJ,
            default         => \PDO::FETCH_BOTH
        };
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
        try {
            // Returns PDOStatement for SELECT/SHOW/EXPLAIN and true/false for others
            $pdo = $this->connect($this->defConnectionName);
            $result = $pdo->query($query);
            return $result;
        } catch (\PDOException $e) {
            // Optional: Custom reconnect logic can go here, if desired
            throw $e;
        }
    }

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
        $prefix = $this->prefix ?? '';
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
     * Executes a raw SQL query with optional parameter binding.
     * - SELECT: returns result rows (array/object/json, je nach Einstellung)
     * - INSERT: returns last insert id
     * - UPDATE/DELETE: returns affected row count
     * - Always sets $this->count, $this->_lastQuery, $this->_stmtError, etc.
     *
     * @param string $query The SQL query (may contain ? or :name placeholders).
     * @param array|null $bindParams Parameters to bind (positional or named).
     * @return mixed Query result (array, int, etc.) depending on statement type.
     * @throws \PDOException on error.
     */
    public function rawQuery(string $query, array $bindParams = null): mixed
    {
        $query = $this->rawAddPrefix($query);
        $this->_query = $query;
        $this->_lastQuery = $query;
        $pdo = $this->connect($this->defConnectionName);

        try {
            $stmt = $pdo->prepare($query);

            if (is_array($bindParams) && !empty($bindParams)) {
                foreach ($bindParams as $key => $value) {
                    $paramKey = is_int($key) ? $key + 1 : $key;
                    $stmt->bindValue($paramKey, $value);
                }
            }

            $stmt->execute();

            $this->count = $stmt->rowCount();
            $this->_stmtError = null;
            $this->_stmtErrno = null;

            $type = strtoupper(strtok(ltrim($query), " "));

            return match ($type) {
                'SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN' => $this->mapResultByKey($stmt->fetchAll($this->getPdoFetchMode())),
                'INSERT'  => $pdo->lastInsertId() ?: $this->count,
                'UPDATE', 'DELETE', 'REPLACE' => $this->count,
                default   => $this->mapResultByKey($stmt->fetchAll($this->getPdoFetchMode())),
            };
        } catch (\PDOException $e) {
            return $this->handleException($e, 'rawQuery');
        } finally {
            $this->reset();
        }
    }

    /**
     * Executes a raw SQL query and returns only the first result row.
     * Note: Does not automatically add 'LIMIT 1' to the query!
     * Behaves like getOne().
     *
     * @param string $query      SQL query to execute (can use ? or named placeholders).
     * @param array|null $bindParams Parameters to bind (positional or named).
     * @return array|null Returns the first result row as array, or null if no row found.
     * @throws \PDOException on error.
     */
    public function rawQueryOne(string $query, array $bindParams = null): ?array
    {
        $res = $this->rawQuery($query, $bindParams);

        if ($res === false) {
            return null;
        }

        if (is_array($res) && isset($res[0]) && is_array($res[0])) {
            return $res[0];
        }

        return null;
    }

    /**
     * Executes a raw SQL query and returns only the first column from all rows.
     * - If 'LIMIT 1' is present in the query, returns a single value.
     * - Otherwise, returns an array of values from the first column of each row.
     * Similar to getValue().
     *
     * @param string $query      SQL query to execute (can use ? or named placeholders).
     * @param array|null $bindParams Parameters to bind (positional or named).
     * @return mixed Single value (if LIMIT 1) or array of values (otherwise), or null if no result.
     * @throws \PDOException on error.
     */
    public function rawQueryValue(string $query, array $bindParams = null): mixed
    {
        $res = $this->rawQuery($query, $bindParams);

        // No result or invalid structure
        if (!$res || !is_array($res) || !isset($res[0]) || !is_array($res[0])) {
            return null;
        }

        // Determine first column key
        $firstKey = array_key_first($res[0]);
        if ($firstKey === null) {
            return null;
        }

        // Normalize query string for LIMIT detection
        $normalized = preg_replace('/\s+/', ' ', strtolower($query));
        $isLimitOne = preg_match('/limit\s+1(\s*(--.*)?)?;?$/i', $normalized);

        // If LIMIT 1 is present, return single value
        if ($isLimitOne) {
            return $res[0][$firstKey];
        }

        // Otherwise return array of first-column values
        $values = [];
        foreach ($res as $row) {
            if (isset($row[$firstKey])) {
                $values[] = $row[$firstKey];
            }
        }

        return $values;
    }

    /**
     * Sets the key for mapping result arrays.
     * If enabled, results will be mapped like [$keyValue => $row, ...]
     *
     * @param string|null $key Column name, or null to disable mapping.
     * @return static
     */
    public function setReturnKey(?string $key): static
    {
        $this->_returnKey = $key;
        return $this;
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
     * Set a column name to be used as key for result-mapping.
     *
     * @param string|null $key Column name or null to disable.
     * @return static
     */
    public function setMapKey(?string $key): static
    {
        $this->_mapKey = $key;
        return $this;
    }

    /**
     * Alias for setReturnKey(), for compatibility/nicer chaining.
     *
     * @param string $field
     * @return static
     */
    public function returnKey(string $field): static
    {
        return $this->setReturnKey($field);
    }

    /**
     * Maps an array of result rows by the configured mapKey (if set).
     * If no mapKey is set, returns the array unchanged.
     *
     * @param array $rows List of DB rows (as arrays)
     * @return array Mapped array if mapKey is set, else original
     */
    protected function mapResultByKey(array $rows): array
    {
        if ($this->_mapKey === null) {
            return $rows;
        }

        $key = $this->_mapKey;
        $mapped = [];

        foreach ($rows as $row) {
            if (isset($row[$key])) {
                $mapped[$row[$key]] = $row;
            } else {
                $mapped[] = $row; // fallback to indexed
            }
        }

        return $mapped;
    }

    /**
     * Executes a SELECT query, with optional LIMIT/OFFSET, and returns the result rows.
     *
     * @param string $query The SQL query (should be a SELECT statement).
     * @param int|array|null $numRows Optional LIMIT or [offset, count] array.
     * @return bool|array Result rows (may be mapped if returnKey is set).
     * @throws \PDOException On query error.
     */
    public function query(string $query, int|array $numRows = null): bool|array
    {
        try {
            // If $numRows is set, add LIMIT/OFFSET to the query as needed.
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

            $this->count = $stmt->rowCount();
            $this->_lastQuery = $query;

            $rows = $stmt->fetchAll($this->getPdoFetchMode());

            // Optionally map result by returnKey
            return $this->mapResultByKey($rows);

        } catch (\PDOException $e) {
            return $this->handleException($e, 'query');
        } finally {
            $this->reset();
        }
    }

    /**
     * Sets one or multiple SQL query options (e.g. DISTINCT, SQL_CALC_FOUND_ROWS).
     * Allows chaining: $db->setQueryOption(['DISTINCT', 'SQL_CALC_FOUND_ROWS']);
     *
     * @param string|array $options One or multiple query option names.
     * @throws \InvalidArgumentException if an unknown option is provided.
     * @return static
     */
    public function setQueryOption(string|array $options): static
    {
        $allowedOptions = [
            'ALL', 'DISTINCT', 'DISTINCTROW', 'HIGH_PRIORITY', 'STRAIGHT_JOIN', 'SQL_SMALL_RESULT',
            'SQL_BIG_RESULT', 'SQL_BUFFER_RESULT', 'SQL_CACHE', 'SQL_NO_CACHE', 'SQL_CALC_FOUND_ROWS',
            'LOW_PRIORITY', 'IGNORE', 'QUICK', 'MYSQLI_NESTJOIN', 'FOR UPDATE', 'LOCK IN SHARE MODE'
        ];

        foreach ((array) $options as $option) {
            $option = strtoupper($option);

            if (!in_array($option, $allowedOptions, true)) {
                throw new \InvalidArgumentException('Invalid SQL query option: ' . $option);
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
     * Enables SQL_CALC_FOUND_ROWS for the next SELECT query,
     * allowing retrieval of the total number of matching rows (ignoring LIMIT).
     *
     * Usage:
     *   $users = $db->withTotalCount()->get('users', [$offset, $count]);
     *   echo $db->totalCount;
     *
     * @return static
     */
    public function withTotalCount(): static
    {
        $this->setQueryOption('SQL_CALC_FOUND_ROWS');
        return $this;
    }

    /**
     * Performs a SELECT query on a given table.
     *
     * @param string         $tableName  The table to query (prefix is applied automatically).
     * @param int|array|null $numRows    Limit as [offset, count] or just count.
     * @param string|array   $columns    Columns to select (default: '*').
     * @return array|false Returns result set or false on failure.
     */
    public function get(string $tableName, int|array $numRows = null, string|array $columns = '*'): bool|array
    {

        $tableName = $this->_validateTableName($tableName);

        if (preg_match('/^([^\s]+)\s+([^\s]+)$/', trim($tableName), $m)) {
            $table = $m[1];
            $alias = $m[2];
            $this->_tableAlias = $alias;
        } else {
            $table = trim($tableName);
            $alias = null;
            $this->_tableAlias = null;
        }


        $prefixed = $this->prefix . $table;


        $this->_tableName = $alias
            ? "{$prefixed} {$alias}"
            : $prefixed;

        $this->_queryOptions = is_array($columns) ? $columns : [$columns];

        $this->_query = '';

        $this->_buildTable();
        $this->_buildJoin();
        $this->_buildCondition('WHERE', $this->_where);
        $this->_buildGroupBy();
        $this->_buildCondition('HAVING', $this->_having);
        $this->_buildOrderBy();
        $this->_buildLimit($numRows);

        if ($this->_forUpdate) {
            $this->_query .= ' FOR UPDATE';
        } elseif ($this->_lockInShareMode) {
            $this->_query .= ' LOCK IN SHARE MODE';
        }

        try {
            $pdo  = $this->connect($this->defConnectionName);
            $stmt = $pdo->prepare($this->_query);
            $stmt->execute($this->_getBindValues());

            $this->count     = $stmt->rowCount();
            $this->_lastQuery = $this->replacePlaceHolders($this->_query, $this->_bindParams);
            $rows = $stmt->fetchAll($this->getPdoFetchMode());

            if (in_array('SQL_CALC_FOUND_ROWS', $this->_queryOptions)) {
                $this->totalCount = (int) $pdo->query("SELECT FOUND_ROWS()")->fetchColumn();
            }

            $rows = $this->mapResultByKey($rows);
            return $rows;

        } catch (\PDOException $e) {
            return $this->handleException($e, 'get');
        } finally {
            $this->reset();
        }
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

    /**
     * Returns only the first row from a select query.
     *
     * @param string       $tableName The table name.
     * @param string|array $columns   Columns to select (default: '*').
     * @return array|null             The first row as array, or null if none found.
     */
    public function getOne(string $tableName, string|array $columns = '*'): ?array
    {
        $res = $this->get($tableName, 1, $columns);

        if (is_array($res)) {
            // Return first element regardless of numeric or associative keys
            foreach ($res as $row) {
                return $row;
            }
        }

        return null;
    }

    /**
     * Returns a single column value (or array of values) from a table.
     * If $limit == 1, returns the value directly. Otherwise, returns an array of values.
     *
     * @param string   $tableName Table name.
     * @param string   $column    Column to select.
     * @param int|null $limit     Limit rows (default: 1). If >1, returns array.
     * @return mixed|null         The value(s) or null if not found.
     * @throws \Exception On query failure.
     */
    public function getValue(string $tableName, string $column, ?int $limit = 1): mixed
    {
        $res = $this->arrayBuilder()->get($tableName, $limit, "{$column} AS retval");

        if (empty($res)) {
            return null;
        }

        if ($limit === 1) {
            $first = reset($res);
            return $first['retval'] ?? null;
        }

        $values = [];
        foreach ($res as $row) {
            $values[] = $row['retval'] ?? null;
        }

        return $values;
    }

    /**
     * Inserts a new row into a table.
     *
     * @param string $tableName   Name of the table.
     * @param array  $insertData  Associative array of column => value.
     * @return int Inserted row ID (if auto-increment), or 1 if no auto-increment column exists.
     * @throws \PDOException on failure.
     */
    public function insert(string $tableName, array $insertData): int
    {
        $tableName = $this->_validateTableName($tableName);
        return $this->_buildInsert($tableName, $insertData, 'INSERT');
    }

    /**
     * Inserts multiple rows at once (each row triggers a single insert).
     *
     * @param string $tableName        The table name.
     * @param array $multiInsertData   2D array of row data (each row is an assoc array or, with $dataKeys, an indexed array).
     * @param array|null $dataKeys     Optional: column names (if $multiInsertData uses numeric arrays).
     * @return array|false Returns array of IDs (or 1 if no auto-increment), or false on error (with rollback).
     * @throws \Exception
     */
    public function insertMulti(string $tableName, array $multiInsertData, array $dataKeys = null): array|false
    {
        // If not already in transaction, wrap in one for safety
        $autoCommit = !$this->_transaction_in_progress;
        $ids = [];

        if ($autoCommit) {
            $this->startTransaction();
        }

        foreach ($multiInsertData as $insertData) {
            if ($dataKeys !== null) {
                // Nur wenn $insertData numerisch ist (z. B. [val1, val2])
                if (array_values($insertData) === $insertData) {
                    $insertData = array_combine($dataKeys, $insertData);
                    if ($insertData === false) {
                        $this->_stmtError = "Fehler bei array_combine(): Key-Count und Value-Count stimmen nicht überein.";
                        if ($autoCommit) {
                            $this->rollback();
                        }
                        return false;
                    }
                }
                // Wenn assoziativ + dataKeys angegeben → Entwicklerfehler
                else {
                    $this->_stmtError = "insertMulti(): dataKeys angegeben, aber insertData ist bereits assoziativ.";
                    if ($autoCommit) {
                        $this->rollback();
                    }
                    return false;
                }
            }

            $id = $this->insert($tableName, $insertData);
            if (!$id) {
                $this->_stmtError = $this->_stmtError ?? "insertMulti(): insert() schlug fehl.";
                // 👇 Füge diese Debugzeile hier ein:
                $this->_stmtError .= " | " . $this->getLastError();
                if ($autoCommit) {
                    $this->rollback();
                }
                return false;
            }

            $ids[] = $id;
        }

        if ($autoCommit) {
            $this->commit();
        }

        return $ids;
    }


    public function insertBulk(string $tableName, array $multiRows): int
    {
        if (empty($multiRows)) {
            return 0;
        }
        $tableName = $this->_validateTableName($tableName);
        $columns = array_keys(reset($multiRows));
        $placeholders = '(' . implode(', ', array_fill(0, count($columns), '?')) . ')';
        $allPlaceholders = implode(', ', array_fill(0, count($multiRows), $placeholders));
        $bindParams = [];

        foreach ($multiRows as $row) {
            foreach ($columns as $col) {
                $bindParams[] = $row[$col] ?? null;
            }
        }

        $table = $this->prefix . $tableName;
        $quotedCols = implode(', ', array_map(fn($col) => "`$col`", $columns));
        $sql = "INSERT INTO {$table} ({$quotedCols}) VALUES {$allPlaceholders}";

        $this->_query = $sql;
        $this->_bindParams = $bindParams;

        try {
            $pdo = $this->connect($this->defConnectionName);
            $stmt = $pdo->prepare($sql);
            $stmt->execute($bindParams);

            $this->count = $stmt->rowCount();
            $this->_lastQuery = $this->replacePlaceHolders($sql, $bindParams);


            return $this->count;
        } catch (\PDOException $e) {

            return $this->handleException($e, 'insertBulk');
        } finally {
            $this->reset();
        }
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
     */
    public function replace(string $tableName, array $insertData): int
    {
        $tableName = $this->_validateTableName($tableName);
        return $this->_buildInsert($tableName, $insertData, 'REPLACE');
    }

    /**
     * Checks if at least one record exists in the table that satisfies the previously set WHERE conditions.
     * Die WHERE-Bedingungen müssen vor dem Aufruf dieser Methode mit `where()` gesetzt worden sein.
     *
     * @param string $tableName The name of the database table to check.
     *
     * @return bool True if at least one record exists, false otherwise.
     * @throws \Exception Wenn ein Fehler bei der Abfrage auftritt.
     */
    public function has(string $tableName): bool
    {
        $this->getOne($tableName, '1');
        return $this->count >= 1;
    }

    /**
     * Performs an UPDATE query. Call "where" before to specify conditions.
     *
     * @param string     $tableName Name of the table.
     * @param array      $tableData Associative array of column => value pairs.
     * @param int|null   $numRows   Optional limit on the number of rows to update.
     *
     * @return bool True on success, false on failure.
     * @throws \PDOException On execution failure.
     */
    public function update(string $tableName, array $tableData, ?int $numRows = null): bool
    {
        if ($this->isSubQuery) {
            return false;
        }
        $tableName = $this->_validateTableName($tableName);
        $this->_query = 'UPDATE ' . $this->prefix . $tableName;

        try {
            $stmt = $this->_buildQuery($numRows, $tableData);

            // Expand array parameters before execute
            $this->expandArrayParams();


            $stmt->execute($this->_bindParams);

            $this->count = $stmt->rowCount();
            $this->_stmtError = null;
            $this->_stmtErrno = null;

            return true;
        } catch (\PDOException $e) {
            return $this->handleException($e, 'update');
        } finally {
            $this->reset();
        }
    }

    /**
     * Performs a DELETE query. Be sure to call "where" beforehand.
     *
     * @param string       $tableName Name of the database table.
     * @param int|array|null $numRows Optional limit for the number of rows to delete.
     *
     * @return bool True on success, false on failure.
     * @throws \Exception When used inside a subquery context.
     */
    public function delete(string $tableName, $numRows = null): bool
    {
        if ($this->isSubQuery) {
            throw new \Exception('Delete function cannot be used within a subquery context.');
        }
        $tableName = $this->_validateTableName($tableName);
        $prefix = $this->prefix ?? '';
        $table = $prefix . $tableName;

        if (count($this->_join) > 0) {
            $tableAlias = preg_replace('/.* (.*)/', '$1', $table);
            $this->_query = "DELETE {$tableAlias} FROM {$table}";
        } else {
            $this->_query = "DELETE FROM {$table}";
        }

        try {
            $stmt = $this->_buildQuery($numRows);

            $this->logQuery($this->_query, $this->_bindParams);

            $stmt->execute();

            $this->_stmtError = null;
            $this->_stmtErrno = null;
            $this->count = $stmt->rowCount();
            return true;
        } catch (\PDOException $e) {
            return $this->handleException($e, 'delete');
        } finally {
            $this->reset();
        }
    }

    /**
     * Adds a WHERE condition to the query, chaining multiple conditions with AND/OR.
     *
     * Usage example:
     *  $db->where('id', 7)->where('title', 'MyTitle', '=', 'AND');
     *
     * @param string $whereProp  Column name to filter by.
     * @param mixed  $whereValue Value to compare against. Use 'DBNULL' for IS NULL checks.
     * @param string $operator   Comparison operator, e.g. '=', '<', 'LIKE'. Default '='.
     * @param string $cond       Logical condition to join with previous where clauses ('AND' or 'OR'). Default 'AND'.
     *
     * @return static Returns $this for method chaining.
     */
    public function where(string $column, mixed $value = null, string $operator = '=', string $cond = 'AND')
    {
        // First condition gets no logical connector
        if (count($this->_where) === 0) {
            $cond = '';
        }

        // If only two parameters passed, treat $operator as value and default to '='
        if ($value === null && $operator !== null && !in_array(strtoupper($operator), ['IS', 'IS NOT'])) {
            $value = $operator;
            $operator = '=';
        }

        // Special handling for NULL comparisons
        if (is_null($value)) {
            $op = strtoupper(trim($operator));

            if ($op === '=' || $op === 'IS') {
                $this->_where[] = [$cond, "{$column} IS NULL"];
            } elseif ($op === '!=' || $op === 'IS NOT') {
                $this->_where[] = [$cond, "{$column} IS NOT NULL"];
            } else {
                throw new \InvalidArgumentException("Unsupported NULL comparison operator: {$operator}");
            }

            return $this->_instance ?? $this;
        }

        // Regular condition
        $this->_where[] = [$cond, $column, $operator, $value];

        return $this->_instance ?? $this;
    }

    /**
     * Adds an OR WHERE condition to the query.
     * This method appends a condition joined by OR to existing WHERE clauses.
     *
     * Note: This should be called only after at least one `where()` condition
     *       has been set to avoid invalid SQL syntax.
     *
     * Example usage:
     * ```php
     * $db->where('status', 'active')->orWhere('role', 'admin');
     * ```
     *
     * @param string $whereProp The name of the database column.
     * @param mixed $whereValue The value to compare the column against.
     * @param string $operator The comparison operator (default '=')
     *
     * @return static Returns the current instance for method chaining.
     */
    public function orWhere(string $column, mixed $value = null, string $operator = '='): static
    {
        // If only two parameters passed, treat $operator as value and default to '='
        if ($value === null && $operator !== null && !in_array(strtoupper($operator), ['IS', 'IS NOT'])) {
            $value = $operator;
            $operator = '=';
        }

        // Special handling for NULL comparisons
        if (is_null($value)) {
            $op = strtoupper(trim($operator));

            if ($op === '=' || $op === 'IS') {
                $this->_where[] = ['OR', "{$column} IS NULL"];
            } elseif ($op === '!=' || $op === 'IS NOT') {
                $this->_where[] = ['OR', "{$column} IS NOT NULL"];
            } else {
                throw new \InvalidArgumentException("Unsupported NULL comparison operator: {$operator}");
            }

            return $this->_instance ?? $this;
        }

        // Regular OR condition
        return $this->where($column, $value, $operator, 'OR');
    }

    /**
     * Store columns for ON DUPLICATE KEY UPDATE clause and optionally last insert ID.
     *
     * @param array       $updateColumns Columns to update on duplicate key.
     * @param int|string|null $lastInsertId Optional last insert ID to use.
     *
     * @return static Fluent interface.
     */
    public function onDuplicate(array $updateColumns, $lastInsertId = null): static
    {
        $this->_lastInsertId = $lastInsertId;
        $this->_updateColumns = $updateColumns;
        return $this;
    }




    /**
     * Adds a HAVING condition to the SQL query, with optional operator and logic chaining (AND/OR).
     *
     * Supports special values:
     * - 'DBNULL'     → IS NULL
     * - 'DBNOTNULL'  → IS NOT NULL
     *
     * Example:
     *   $db->having('SUM(score)', 10, '>')->orHaving('COUNT(id)', 'DBNOTNULL');
     *
     * @param string $havingProp   Column or expression for the HAVING clause.
     * @param mixed  $havingValue  Value to compare against, or special string like 'DBNULL'.
     * @param string $operator     Comparison operator (default '=')
     * @param string $cond         Logic condition to join with previous (AND/OR). Default: 'AND'
     *
     * @return static
     */
    public function having(string $havingProp, mixed $havingValue = 'DBNULL', string $operator = '=', string $cond = 'AND'): static
    {
        // Backward compatibility: having(['>' => 10])
        if (is_array($havingValue) && ($key = key($havingValue)) !== 0) {
            $operator = (string)$key;
            $havingValue = $havingValue[$key];
        }

        // Special handling for NULL
        if ($havingValue === 'DBNULL') {
            $operator = 'IS';
            $havingValue = 'NULL';
        } elseif ($havingValue === 'DBNOTNULL') {
            $operator = 'IS NOT';
            $havingValue = 'NULL';
        }

        // Validate operator (optional strict list)
        $allowedOperators = ['=', '!=', '<>', '<', '<=', '>', '>=', 'LIKE', 'NOT LIKE', 'IS', 'IS NOT', 'IN', 'NOT IN', 'BETWEEN'];
        if (!in_array(strtoupper($operator), $allowedOperators, true)) {
            throw new \InvalidArgumentException("Invalid HAVING operator: $operator");
        }

        // First clause: drop condition keyword
        if (count($this->_having) === 0) {
            $cond = '';
        }

        $this->_having[] = [$cond, $havingProp, $operator, $havingValue];
        return $this;
    }

    /**
     * Shortcut for a HAVING clause joined with OR.
     *
     * @param string $prop
     * @param mixed  $value
     * @param string $operator
     * @return static
     */
    public function orHaving(string $prop, mixed $value = 'DBNULL', string $operator = '='): static
    {
        if (count($this->_having) === 0) {
            throw new \LogicException("orHaving() cannot be used without a prior having() clause.");
        }
        return $this->having($prop, $value, $operator, 'OR');
    }

    /**
     * Adds a JOIN clause to the query.
     *
     * Supports simple joins and subqueries:
     *   $db->join('users u', 'u.id = p.user_id', 'LEFT')
     *   $db->join($db->subQuery('orders'), 'o.user_id = u.id', 'INNER')
     *
     * @param string|object $joinTable     Table name or subquery (as object with __toString).
     * @param string        $joinCondition SQL ON condition, e.g. 'a.id = b.ref_id'.
     * @param string        $joinType      Join type: 'LEFT', 'INNER', etc. (optional).
     *
     * @return static
     * @throws \InvalidArgumentException if join type is invalid.
     */

    public function join(string $joinTable, string $joinCondition, string $joinType = 'LEFT'): static
    {
        try {
            $joinTable = $this->_validateTableName($joinTable);
        } catch (\InvalidArgumentException $e) {
            $this->logException($e, "join [table={$joinTable}]");
            throw $e;
        }

        $this->_pendingJoins[] = [$joinType, $joinTable, $joinCondition];
        return $this;
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
    protected function addTicks(string $field): string
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
     * Imports raw CSV data into a database table using LOAD DATA INFILE.
     *
     * @param string      $importTable    The database table to import data into.
     * @param string      $importFile     The file path to the CSV file.
     * @param array|null  $importSettings Optional import settings:
     *                                    - fieldChar: field delimiter (default ';')
     *                                    - lineChar: line delimiter (default PHP_EOL)
     *                                    - linesToIgnore: number of lines to skip (default 1)
     *                                    - fieldEnclosure: optional enclosure character (e.g. '"')
     *                                    - lineStarting: optional line start delimiter
     *                                    - loadDataLocal: bool, whether to use LOCAL keyword
     *
     * @return bool True on successful import, false on failure.
     * @throws \Exception If the file does not exist or on query errors.
     */
    public function loadData(string $importTable, string $importFile, ?array $importSettings = null): bool
    {
        // Verify file existence
        if (!file_exists($importFile)) {
            throw new \Exception("Import file '{$importFile}' does not exist.");
        }

        // Default import settings
        $settings = [
            'fieldChar' => ';',
            'lineChar' => PHP_EOL,
            'linesToIgnore' => 1,
        ];

        // Merge user settings if provided
        if (is_array($importSettings)) {
            $settings = array_merge($settings, $importSettings);
        }
        $prefix = $this->prefix ?? '';
        // Add table prefix if any
        $table = $prefix . $importTable;

        // Escape backslashes for SQL
        $escapedImportFile = str_replace("\\", "\\\\", $importFile);

        // Use LOCAL keyword if requested
        $loadDataLocal = !empty($settings['loadDataLocal']) ? 'LOCAL' : '';

        // Build the LOAD DATA SQL statement
        $sql = sprintf(
            "LOAD DATA %s INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '%s'",
            $loadDataLocal,
            $escapedImportFile,
            $table,
            $settings['fieldChar']
        );

        if (!empty($settings['fieldEnclosure'])) {
            $sql .= " ENCLOSED BY '" . $settings['fieldEnclosure'] . "'";
        }

        $sql .= " LINES TERMINATED BY '" . $settings['lineChar'] . "'";

        if (!empty($settings['lineStarting'])) {
            $sql .= " STARTING BY '" . $settings['lineStarting'] . "'";
        }

        $sql .= " IGNORE " . (int)$settings['linesToIgnore'] . " LINES";

        // Execute the query with error handling
        try {
            $result = $this->queryUnprepared($sql);
            return (bool)$result;
        } catch (\Exception $e) {
            // Optional: log the error here if needed
            // error_log("LoadData error: " . $e->getMessage());
            // Re-throw or return false depending on your preference
            throw $e;
        }
    }

    /**
     * Imports XML data into a database table using LOAD XML INFILE.
     *
     * @param string      $importTable    The table into which data will be imported.
     * @param string      $importFile     The XML file path.
     * @param array|null  $importSettings Optional settings:
     *                                    - linesToIgnore: int, lines to skip (default 0)
     *                                    - rowTag: string, the XML row tag identifier
     *
     * @return bool True on successful import, false otherwise.
     * @throws \Exception If the import file does not exist or on query errors.
     */
    public function loadXml(string $importTable, string $importFile, ?array $importSettings = null): bool
    {
        // Check if the import file exists
        if (!file_exists($importFile)) {
            throw new \Exception("loadXml: Import file '{$importFile}' does not exist.");
        }

        // Default settings
        $settings = [
            'linesToIgnore' => 0,
        ];

        // Merge user settings if provided
        if (is_array($importSettings)) {
            $settings = array_merge($settings, $importSettings);
        }
        $prefix = $this->prefix ?? '';
        // Add table prefix if any
        $table = $prefix . $importTable;

        // Escape backslashes for SQL compatibility
        $escapedImportFile = str_replace("\\", "\\\\", $importFile);

        // Build LOAD XML query
        $sql = sprintf("LOAD XML INFILE '%s' INTO TABLE %s", $escapedImportFile, $table);

        if (!empty($settings['rowTag'])) {
            $sql .= " ROWS IDENTIFIED BY '" . $settings['rowTag'] . "'";
        }

        $sql .= " IGNORE " . (int)$settings['linesToIgnore'] . " LINES";

        // Execute unprepared statement with error handling
        try {
            $result = $this->queryUnprepared($sql);
            return (bool)$result;
        } catch (\Exception $e) {
            // Optional: log error here
            // error_log("loadXml error: " . $e->getMessage());
            throw $e;
        }
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
     * @throws \InvalidArgumentException on invalid inputs.
     */
    public function orderBy(string $orderByField, string $orderbyDirection = 'DESC', array|string|null $customFieldsOrRegExp = null): static
    {
        $allowedDirections = ['ASC', 'DESC'];
        $orderbyDirection = strtoupper(trim($orderbyDirection));

        if (!in_array($orderbyDirection, $allowedDirections, true)) {
            throw new \InvalidArgumentException("Invalid order direction: {$orderbyDirection}");
        }

        // Clean field name
        $orderByField = preg_replace("/[^ -a-z0-9\.\(\),_`\*'\" ]+/i", '', $orderByField);

        // Prefixing (only if no schema part exists)
        $prefix = $this->prefix ?? '';
        if (!str_contains($orderByField, '.')) {
            $orderByField = $prefix . $orderByField;
        } elseif (preg_match('/^`([a-zA-Z0-9_]+)`\.`([a-zA-Z0-9_]+)`$/', $orderByField, $m)) {
            $orderByField = '`' . $prefix . $m[1] . '`.`' . $m[2] . '`';
        }

        if (is_array($customFieldsOrRegExp)) {
            // FIELD() custom order
            $customFieldsOrRegExp = array_map(fn($v) => preg_replace("/[^\x80-\xffa-z0-9\.\(\),_` ]+/i", '', $v), $customFieldsOrRegExp);
            $orderByField = 'FIELD(' . $orderByField . ', "' . implode('","', $customFieldsOrRegExp) . '")';

        } elseif (is_string($customFieldsOrRegExp)) {
            if (!preg_match('/^[\w\s\^\$\.\*\+\?\|\[\]\(\)]+$/', $customFieldsOrRegExp)) {
                throw new \InvalidArgumentException("Invalid regular expression: {$customFieldsOrRegExp}");
            }
            $orderByField .= " REGEXP '" . $customFieldsOrRegExp . "'";

        } elseif ($customFieldsOrRegExp !== null) {
            throw new \InvalidArgumentException("Invalid custom field or REGEXP value: " . print_r($customFieldsOrRegExp, true));
        }

        $this->_orderBy[$orderByField] = $orderbyDirection;
        return $this;
    }


    /**
     * Adds a GROUP BY clause to the query.
     * Supports multiple calls for chaining multiple GROUP BY fields.
     *
     * @param string $groupByField The database field or expression to group by.
     *
     * @return static Returns the current instance for method chaining.
     */
    public function groupBy(string $groupByField): static
    {
        // Sanitize input to allow only common SQL characters in group by expressions
        $groupByField = preg_replace("/[^-a-z0-9\.\(\),_\* <>=!]+/i", '', $groupByField);

        $this->_groupBy[] = $groupByField;

        return $this;
    }

    /**
     * Sets the current table lock method.
     *
     * @param string $method The table lock method. Allowed values: 'READ' or 'WRITE'.
     *
     * @throws \InvalidArgumentException If an invalid lock method is provided.
     * @return static Returns the current instance for method chaining.
     */
    public function setLockMethod(string $method): static
    {
        $method = strtoupper($method);
        if ($method === 'READ' || $method === 'WRITE') {
            $this->_tableLockMethod = $method;
        } else {
            throw new \InvalidArgumentException("Invalid lock type: must be either 'READ' or 'WRITE'.");
        }

        return $this;
    }

    /**
     * Locks one or more tables for read/write operations.
     *
     * @param string|string[] $table Table name or array of table names to lock.
     * @return bool True on success, false on failure.
     */
    public function lock(string|array $table): bool
    {
        $prefix = $this->prefix ?? '';
        $this->_query = "LOCK TABLES ";

        if (is_array($table)) {
            $parts = [];
            foreach ($table as $value) {
                if (is_string($value)) {
                    try {
                        $validated = $this->_validateTableName($value);
                        $parts[] = $prefix . $validated . ' ' . $this->_tableLockMethod;
                    } catch (\Throwable $e) {
                        $this->logException($e, "lock [table={$value}]");
                        throw $e;
                    }
                }
            }
            $this->_query .= implode(', ', $parts);
        } else {
            try {
                $validated = $this->_validateTableName($table);
                $this->_query .= $prefix . $validated . ' ' . $this->_tableLockMethod;
            } catch (\Throwable $e) {
                $this->logException($e, "lock [table={$table}]");
                throw $e;
            }
        }

        try {
            $result = $this->queryUnprepared($this->_query);
            $this->reset();
            return (bool)$result;
        } catch (\Throwable $e) {
            return $this->handleException($e, 'lock');
        }
    }

    /**
     * Unlocks all tables in the current database connection.
     * Also commits any pending transactions.
     *
     * @return static Returns the current instance for chaining.
     * @throws \Exception If unlocking fails.
     */
    public function unlock(): static
    {
        $this->_query = "UNLOCK TABLES";

        try {
            $result = $this->queryUnprepared($this->_query);
            $this->reset();

            if ($result) {
                return $this;
            } else {
                throw new \Exception("Unlocking tables failed.");
            }
        } catch (\Exception $e) {
            $this->reset();
            throw $e;
        }
    }

    /**
     * Returns the ID of the last inserted item.
     *
     * @return string The last inserted item ID.
     */
    public function getInsertId(): string
    {
        try {
            $pdo = $this->connect($this->defConnectionName);
            return $pdo->lastInsertId();
        } catch (\PDOException $e) {
            return $this->handleException($e, 'getInsertID');
        }
    }

    /**
     * Dummy escape method for compatibility.
     * Use prepared statements and parameter binding instead!
     *
     * @param string $str Input string.
     * @return string The input string untouched.
     */
    public function escape(string $str): string
    {
        // Could use addslashes($str) but better to encourage prepared statements
        return $str;
    }

    /*
    * if you want a real escape you must unkomment the dummy method

        public function escape(string $str): string
        {
            $pdo = $this->connect($this->defConnectionName);
            $quoted = $pdo->quote($str);
            // quote() gibt den String mit Anführungszeichen zurück, z.B. 'abc', die wollen wir entfernen:
            return substr($quoted, 1, -1);
        }

     *
     */

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
     * Determines a legacy type character for a given PHP value.
     * Used for internal compatibility (e.g. legacy binding logic).
     *
     * @param mixed $item The value to check.
     * @return string One of:
     *                's' => string or null,
     *                'i' => int or bool,
     *                'd' => float (double),
     *                'b' => object (e.g. blobs),
     *                ''  => unknown or unsupported.
     */
    protected function _determineType($item): string
    {
        return match (gettype($item)) {
            'string', 'NULL'   => 's',
            'integer', 'boolean' => 'i',
            'double'           => 'd',
            'object'           => 'b',
            default            => '',
        };
    }

    /**
     * Adds a value to the internal parameter bind list, along with its PDO type.
     * Used internally for prepared statement parameter binding.
     *
     * @param mixed $value The value to be bound.
     * @return void
     */
    protected function _bindParam(mixed $value): void
    {
        static $typeMap = [
            'integer' => \PDO::PARAM_INT,
            'boolean' => \PDO::PARAM_BOOL,
            'NULL'    => \PDO::PARAM_NULL,
            // Alles andere → STRING
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

    /**
     * Builds a SQL condition pair for WHERE/HAVING clauses.
     *
     * - If a scalar value is provided, adds it as a bound parameter (`?`) and returns a placeholder expression.
     * - If an object is passed, it must implement a `getSubQuery()` method which returns:
     *   ['query' => '...', 'params' => [...], 'alias' => 'AS x']
     *   In this case, the subquery SQL is embedded and its parameters are appended to the bindings.
     *
     * Example:
     *   $db->_buildPair('=', 5); → " = ? " (and adds value 5 to bound parameters)
     *   $db->_buildPair('IN', $subQueryObj); → " IN (SELECT ...) AS x "
     *
     * @param string $operator SQL comparison operator (e.g. '=', 'IN', '>', etc.).
     * @param mixed $value A scalar value or an object that represents a subquery.
     *
     * @return string SQL fragment for the condition.
     * @throws \InvalidArgumentException If a subquery object lacks getSubQuery().
     * @throws \UnexpectedValueException If subquery structure is invalid.
     */
    protected function _buildPair(string $operator, mixed $value): string
    {
        // Normaler Wert → einfach binden
        if (!is_object($value)) {
            $this->_bindParam($value);
            return " {$operator} ? ";
        }

        // SubQuery-Objekt prüfen
        if (!method_exists($value, 'getSubQuery')) {
            throw new \InvalidArgumentException('Subquery object must implement getSubQuery method');
        }

        $subQuery = $value->getSubQuery();

        if (!is_array($subQuery) || !isset($subQuery['query'], $subQuery['params'])) {
            throw new \UnexpectedValueException('Subquery must return array with keys: query, params');
        }

        $this->_bindParams($subQuery['params']);

        // Kein Alias hier erlaubt!
        return " {$operator} ({$subQuery['query']}) ";
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
     * @throws \Exception If called within subquery context or preparation fails.
     */
    protected function _buildInsert(string $tableName, array $insertData, string $operation): int|bool
    {
        if ($this->isSubQuery) {
            throw new \Exception("Cannot perform insert inside a subquery context.");
        }

        $prefix = $this->prefix ?? '';
        $table = $prefix . $tableName;

        $columns = implode(', ', array_map([$this, 'addTicks'], array_keys($insertData)));
        $placeholders = implode(', ', array_fill(0, count($insertData), '?'));
        $options = !empty($this->_queryOptions) ? implode(' ', $this->_queryOptions) . ' ' : '';

        $this->_query = sprintf('%s %sINTO %s (%s) VALUES (%s)', $operation, $options, $table, $columns, $placeholders);
        $this->_bindParams = array_values($insertData);

        // Optional: ON DUPLICATE
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
            $this->count = $stmt->rowCount();
            $this->_stmtError = null;
            $this->_stmtErrno = null;

            $haveOnDuplicate = !empty($this->_updateColumns);

            if ($this->count < 1) {
                return $haveOnDuplicate ? true : false;
            }

            return ($operation === 'INSERT' && $pdo->lastInsertId()) ? (int) $pdo->lastInsertId() : true;

        } catch (\PDOException $e) {
            return $this->handleException($e, '_buildInsert');
        } finally {
            $this->reset();
        }
    }


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
     * @return \PDOStatement Prepared PDO statement.
     * @throws \Exception On preparation failure or invalid context.
     */
    protected function _buildQuery(int|array|null $numRows = null, array $tableData = null): \PDOStatement
    {
        if ($this->isSubQuery) {
            throw new \Exception("_buildQuery() should not be called inside a subquery.");
        }

        // Query-Typ bestimmen (INSERT / REPLACE / UPDATE → handled separat)
        $this->_buildInsertQuery($tableData);

        // Neu: SELECT-Fälle – erst FROM, dann JOIN
        $this->_buildTable();     // ← baut "SELECT x FROM y"
        $this->_buildJoin();      // ← hängt JOINs korrekt danach an

        $this->_buildCondition('WHERE', $this->_where);
        $this->_buildGroupBy();
        $this->_buildCondition('HAVING', $this->_having);
        $this->_buildOrderBy();
        $this->_buildLimit($numRows);
        $this->_buildOnDuplicate($tableData);

        if ($this->_forUpdate) {
            $this->_query .= ' FOR UPDATE';
        }
        if ($this->_lockInShareMode) {
            $this->_query .= ' LOCK IN SHARE MODE';
        }

        $this->_lastQuery = $this->replacePlaceHolders($this->_query, $this->_bindParams);

        try {
            $pdo = $this->connect($this->defConnectionName);
            $stmt = $pdo->prepare($this->_query);

            foreach ($this->_bindParams as $idx => $bind) {
                if (is_array($bind) && isset($bind['value'], $bind['type'])) {
                    $stmt->bindValue(is_int($idx) ? $idx + 1 : $idx, $bind['value'], $bind['type']);
                } else {
                    $stmt->bindValue(is_int($idx) ? $idx + 1 : $idx, $bind);
                }
            }

            return $stmt;
        } catch (\PDOException $e) {
            throw new \Exception("Failed to prepare query: " . $e->getMessage(), (int)$e->getCode());
        }
    }


    /**
     * Deprecated: Old internal method for dynamic mysqli result binding.
     * Not required with PDO. Use PDO::fetchAll with desired fetch mode.
     *
     * @throws \LogicException always
     */
    protected function _dynamicBindResults($stmt)
    {
        throw new \LogicException('Deprecated: use PDO::fetchAll with defined fetch mode.');
    }

    protected function _buildJoinOld(): void
    {
        throw new \LogicException('_buildJoinOld() is deprecated and should not be used.');
    }

    /**
     * Helper method to build SQL fragments for INSERT and UPDATE statements.
     * Handles bindings, subqueries, inline expressions, and special flags.
     *
     * @param array $tableData    Associative array of data for query building.
     * @param array $tableColumns List of keys (columns) to use.
     * @param bool  $isInsert     True = INSERT, false = UPDATE.
     *
     * @throws \Exception If input is invalid or special format fails.
     */
    protected function _buildDataPairs(array $tableData, array $tableColumns, bool $isInsert): void
    {
        foreach ($tableColumns as $column) {
            if (!array_key_exists($column, $tableData)) {

                throw new \Exception("Column '{$column}' missing in data array.");
            }

            $value = $tableData[$column];

            if (!$isInsert) {
                $this->_query .= $this->addTicks($column) . " = ";
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
                        $this->_query .= $this->addTicks($column) . " {$val}, ";
                        break;

                    case '[F]':
                        if (!is_array($val) || !isset($val[0])) {
                            throw new \Exception("Invalid [F] format for column '{$column}'");
                        }
                        $this->_query .= $val[0] . ", ";
                        if (!empty($val[1])) {
                            $this->_bindParams($val[1]);
                        }
                        break;

                    case '[N]':
                        $escapedValue = is_string($val)
                            ? $this->addTicks($val)
                            : $this->addTicks($column);
                        $this->_query .= '!' . $escapedValue . ', ';
                        break;

                    default:
                        throw new \Exception("Invalid operation flag: {$flag} in column '{$column}'");
                }
            }
        }

        $this->_query = rtrim($this->_query, ', ');
    }

    /**
     * Appends ON DUPLICATE KEY UPDATE clause to current query.
     *
     * @param array $tableData Full insert/update data (used to extract update values)
     * @return void
     * @throws \Exception
     */
    protected function _buildOnDuplicate(array $tableData): void
    {
        if (empty($this->_updateColumns) || !is_array($this->_updateColumns)) {
            return;
        }

        $this->_query .= " ON DUPLICATE KEY UPDATE ";

        // Optional: LAST_INSERT_ID(column) Hack – e.g. for tracking affected row
        if (!empty($this->_lastInsertId)) {
            $safeColumn = $this->addTicks($this->_lastInsertId);
            $this->_query .= "{$safeColumn} = LAST_INSERT_ID({$safeColumn}), ";
        }

        // Normalize updateColumns to ["column" => value]
        $updatePairs = [];

        foreach ($this->_updateColumns as $key => $val) {
            if (is_int($key)) {
                $column = $val;
                $updatePairs[$column] = $tableData[$column] ?? null;
            } else {
                $updatePairs[$key] = $val;
            }
        }

        $this->_buildDataPairs($updatePairs, array_keys($updatePairs), false);
    }

    /**
     * Internal method that builds the INSERT, REPLACE, or UPDATE part of the SQL query.
     * Delegates detailed value handling to _buildDataPairs().
     *
     * @param array $tableData Associative array of column => value pairs.
     *
     * @throws \InvalidArgumentException If the data array is empty.
     * @throws \RuntimeException If the current query type is not recognized.
     * @throws \Exception On internal query formatting issues.
     */
    protected function _buildInsertQuery(array $tableData): void
    {
        if (empty($tableData)) {
            throw new \InvalidArgumentException("Empty data array passed to _buildInsertQuery().");
        }

        $queryType = strtoupper(strtok(trim($this->_query), ' '));
        $isInsert = in_array($queryType, ['INSERT', 'REPLACE'], true);

        if (!$isInsert && $queryType !== 'UPDATE') {
            throw new \RuntimeException("Unsupported query type in _buildInsertQuery(): '{$queryType}'");
        }

        $dataColumns = array_keys($tableData);
        $escapedColumns = [];

        foreach ($dataColumns as $col) {
            $escapedColumns[] = $this->addTicks($col);
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


    protected function _buildCondition(string $operator, array &$conditions): void
    {
        if (empty($conditions)) {
            return;
        }

        $this->_query .= ' ' . $operator;

        foreach ($conditions as $index => [$concat, $column, $comparison, $value]) {

            if ($index > 0 && $concat) {
                $this->_query .= " {$concat}";
            }

            $comparisonLower = strtolower($comparison);

            switch ($comparisonLower) {
                case 'in':
                case 'not in':
                    if (!is_array($value) || empty($value)) {
                        throw new \InvalidArgumentException("IN condition requires non-empty array of values");
                    }

                    $placeholders = [];
                    foreach ($value as $v) {
                        $placeholders[] = '?';
                        $this->_bindParam($v);
                    }

                    $this->_query .= ' ' . $this->addTicks($column) . " {$comparison} (" . implode(', ', $placeholders) . ")";
                    break;

                case 'between':
                case 'not between':
                    if (!is_array($value) || count($value) !== 2) {
                        throw new \InvalidArgumentException("BETWEEN requires exactly two values");
                    }
                    $this->_bindParams($value);
                    $this->_query .= ' ' . $this->addTicks($column) . " {$comparison} ? AND ?";
                    break;

                case 'exists':
                case 'not exists':
                    if (!is_object($value) || !method_exists($value, 'getSubQuery')) {
                        throw new \InvalidArgumentException("{$comparison} requires valid subquery object");
                    }

                    $sub = $value->getSubQuery();
                    if (!isset($sub['query'], $sub['params'])) {
                        throw new \UnexpectedValueException("Subquery must return [query, params]");
                    }

                    $this->_bindParams($sub['params']);
                    $this->_query .= " {$comparison} ({$sub['query']})";
                    break;

                default:
                    if ($value === null) {
                        $this->_query .= ' ' . $this->addTicks($column) . " {$comparison} NULL";
                    } elseif (is_array($value)) {
                        $this->_query .= ' ' . $this->addTicks($column) . " {$comparison}" . str_repeat(' ?,', count($value) - 1) . ' ?';
                        $this->_bindParams($value);
                    } else {
                        $this->_query .= ' ' . $this->addTicks($column) . $this->_buildPair($comparison, $value);
                    }
                    break;
            }
        }
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
            // Allow raw expressions like COUNT(*)
            if (str_contains($field, '(') || str_contains($field, '`')) {
                return $field;
            }
            return $this->addTicks($field);
        }, $this->_groupBy);

        $this->_query .= ' GROUP BY ' . implode(', ', $escaped);
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
                : $this->addTicks($column);

            $clauses[] = "$escapedColumn $direction";
        }

        $this->_query .= ' ORDER BY ' . implode(', ', $clauses) . ' ';
    }

    /**
     * Internal helper to build the LIMIT clause of the SQL query.
     *
     * Accepts either a single integer for row count, or an array [offset, count].
     *
     * @param int|array|null $numRows Optional row limit or offset/count pair.
     * @return void
     */
    protected function _buildLimit(int|array|null $numRows): void
    {
        if ($numRows === null) {
            return;
        }

        if (is_array($numRows)) {
            if (count($numRows) !== 2) {
                throw new \InvalidArgumentException('Limit array must contain exactly two elements: [offset, count]');
            }

            [$offset, $count] = array_map('intval', $numRows);

            if ($count < 0 || $offset < 0) {
                throw new \InvalidArgumentException('Offset and limit must be non-negative integers');
            }

            $this->_query .= " LIMIT {$offset}, {$count}";
        } else {
            $limit = (int) $numRows;

            if ($limit < 0) {
                throw new \InvalidArgumentException('Limit must be a non-negative integer');
            }

            $this->_query .= " LIMIT {$limit}";
        }
    }

    /**
     * Debug helper: replaces ? placeholders in SQL string for logging purposes only.
     * NEVER use this result to execute SQL!
     *
     * @param string $query  The SQL query with `?` placeholders.
     * @param array  $params The values to inject into the placeholders.
     * @return string The SQL string with values interpolated.
     */
    protected function replacePlaceHolders(string $query, array $params): string
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
                        $escaped = array_map(
                            fn($v) => is_string($v) ? "'" . addslashes($v) . "'" : (is_null($v) ? "NULL" : $v),
                            $value
                        );
                        $result .= implode(', ', $escaped);
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

    /**
     * Returns the last executed SQL query with bound parameters interpolated.
     *
     * Useful for debugging purposes. Do not use the returned string for execution.
     *
     * @return string The last executed SQL query as a string.
     */
    public function getLastQuery(): string
    {
        return $this->_lastQuery;
    }

    /**
     * Returns the last error message from the last executed statement or connection.
     *
     * @return string Error message, or 'No error' if none occurred.
     */
    public function getLastError(): string
    {
        if (!isset($this->_pdo[$this->defConnectionName])) {
            return "PDO connection is null";
        }

        $errorParts = [];

        if (!empty($this->_stmtError)) {
            $errorParts[] = trim($this->_stmtError);
        }

        try {
            $pdo = $this->connect($this->defConnectionName);
            $pdoError = $pdo->errorInfo();
            if (!empty($pdoError[2])) {
                $errorParts[] = trim($pdoError[2]);
            }
        } catch (\Throwable $e) {
            $errorParts[] = $e->getMessage();
        }

        return trim($errorParts ? implode(" | ", array_unique($errorParts)) : "No error");
    }


    /**
     * Returns the last error code from the executed PDO statement.
     *
     * @return int PDO error code, or 0 if none.
     */
    public function getLastErrno(): int
    {
        return $this->_stmtErrno ?? 0;
    }

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

        $result = [
            'query'  => $this->_query,
            'params' => $this->_bindParams,
            'alias'  => $this->connectionsSettings[$this->defConnectionName]['host'] ?? null
        ];

        $this->reset();
        return $result;
    }

    /**
     * Returns an SQL-compatible interval expression string for use in WHERE clauses or arithmetic operations.
     *
     * Examples:
     *   $this->interval('-1d')           → NOW() - interval 1 day
     *   $this->interval('10m', 'ts')     → ts + interval 10 minute
     *
     * Supported shorthand types:
     *   's' → second, 'm' → minute, 'h' → hour,
     *   'd' → day,    'M' → month,  'Y' → year
     *
     * @param string $diff Interval definition in formats like "10m", "-1d", "+ 2 hour"
     * @param string $func Base SQL function or column name (default: NOW())
     * @return string SQL snippet (e.g., "NOW() + interval 10 minute")
     * @throws \Exception If interval type or format is invalid
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
                throw new \Exception("Invalid interval unit '{$unit}' in '{$diff}'");
            }

            if ($amount === '') {
                throw new \Exception("Missing amount in interval definition '{$diff}'");
            }

            return "{$func} {$incr} interval {$amount} {$types[$unit]}";
        }

        return $func;
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
        return ['[F]' => [$this->interval($diff, $func)]];
    }

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
     * Returns an SQL-compatible expression to increment a numeric field.
     *
     * Usage in an update statement:
     *   $db->update('table', ['views' => $db->inc(1)]);
     *   → Results in SQL: `views = views + 1`
     *
     * @param int|float $num Amount to increment by (default: 1)
     * @return array<string, string> SQL-compatible update expression
     * @throws \InvalidArgumentException If the value is not numeric.
     */
    public function inc(int|float $num = 1): array
    {
        if (!is_numeric($num)) {
            throw new \InvalidArgumentException('Argument supplied to inc() must be numeric.');
        }

        return ['[I]' => ' + ' . $num];
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
     * @throws \InvalidArgumentException
     */
    public function dec(int|float $num = 1): array
    {
        if (!is_numeric($num)) {
            throw new \InvalidArgumentException('Argument supplied to dec() must be numeric.');
        }

        return ['[I]' => ' - ' . $num];
    }

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
     * @return static Instance configured as subquery
     */
    public static function subQuery(string $subQueryAlias = ''): static
    {
        return new static([
            'host' => $subQueryAlias,
            'isSubQuery' => true
        ]);
    }

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
     * Begins a new database transaction using PDO.
     *
     * @throws \Exception if transaction start fails
     */
    public function startTransaction(): void
    {
        $pdo = $this->connect($this->defConnectionName);

        if ($pdo->inTransaction()) {
            throw new \RuntimeException("Transaction already active");
        }

        if (!$pdo->beginTransaction()) {
            throw new \RuntimeException("Unable to begin transaction");
        }

        $this->_transaction_in_progress = true;
    }

    /**
     * Commits the current transaction.
     *
     * @throws \Exception if no transaction is active or commit fails.
     */
    public function commit(): void
    {
        $pdo = $this->connect($this->defConnectionName);

        if (!$pdo->inTransaction()) {
            throw new \Exception("No active transaction to commit");
        }

        if (!$pdo->commit()) {
            throw new \Exception("Transaction commit failed");
        }

        $this->_transaction_in_progress = false;
    }

    /**
     * Rolls back the current transaction.
     *
     * @throws \Exception if no transaction is active or rollback fails.
     */
    public function rollback(): void
    {
        $pdo = $this->connect($this->defConnectionName);

        if (!$pdo->inTransaction()) {
            throw new \Exception("No active transaction to rollback");
        }

        if (!$pdo->rollBack()) {
            throw new \Exception("Transaction rollback failed");
        }

        $this->_transaction_in_progress = false;
    }

    /**
     * Shutdown handler to rollback uncommitted operations in order to keep
     * atomic operations sane. Only called when a transaction was left open.
     */
    public function _transaction_status_check(): void
    {
        try {
            if ($this->_transaction_in_progress) {
                $this->rollback();
            }
        } catch (\Throwable $e) {
            error_log("Shutdown rollback failed: " . $e->getMessage());
        }
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
    public function setTrace(bool $enabled, string $stripPrefix = ''): static
    {
        $this->traceEnabled = $enabled;
        $this->traceStripPrefix = $stripPrefix;
        return $this;
    }

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
     * Checks if the given table(s) exist in the currently selected database.
     *
     * @param string|array $tables One or multiple table names to check.
     * @return bool True if all tables exist, false otherwise.
     * @throws \Exception
     */
    public function tableExists(string|array $tables): bool
    {
        $tables = (array)$tables;
        if (empty($tables)) {
            return false;
        }

        $dbName = $this->connectionsSettings[$this->defConnectionName]['db'] ?? null;
        if (!$dbName) {
            throw new \Exception("No database selected for connection '{$this->defConnectionName}'");
        }

        $prefix = $this->prefix ?? '';

        // ✅ Tabelle(n) validieren und prefixen
        $validatedTables = [];
        foreach ($tables as $t) {
            $validated = $this->_validateTableName($t);
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
     * Configures the query to return results as an associative array indexed by the given field.
     *
     * Example:
     *   $db->map('id')->get('users');
     *   // returns: [1 => [...], 2 => [...], ...]
     *
     * @param string $idField The field name to use as array key in result set.
     * @return static
     */
    public function map(string $idField): static
    {
        return $this->setMapKey($idField);
    }

    /**
     * Pagination wrapper for get() queries.
     *
     * Automatically calculates the correct offset and sets totalPages
     * based on the total count returned by SQL_CALC_FOUND_ROWS or similar logic.
     *
     * Example:
     *   $results = $db->paginate('users', 2, ['id', 'name']);
     *
     * @param string       $table  The name of the database table.
     * @param int          $page   Page number (1-based).
     * @param array|string $fields Fields to fetch (array or comma-separated string).
     *
     * @return array Returns result set for the requested page.
     * @throws \Exception
     */
    public function paginate(string $table, int $page, array|string $fields = ['*']): array
    {
        if (!$this->pageLimit || $this->pageLimit < 1) {
            throw new \RuntimeException('Invalid page limit. Please set a positive value for $pageLimit.');
        }
        $table = $this->_validateTableName($table);
        $offset = $this->pageLimit * max(0, $page - 1);

        $res = $this
            ->withTotalCount()
            ->get($table, [$offset, $this->pageLimit], $fields);

        $this->totalPages = (int) ceil($this->totalCount / $this->pageLimit);

        return $res;
    }

    /**
     * Adds a conditional ON-clause to a previously defined JOIN.
     *
     * This allows method-chained, prefixed WHERE-style conditions on JOINed tables.
     *
     * Example:
     *   $db->join('user u', 'u.role_id = r.id', 'LEFT')
     *      ->joinWhere('user u', 'u.active', 1)
     *      ->joinWhere('user u', 'u.deleted', 0);
     *
     * @param string      $whereJoin   The joined table name with optional alias (e.g. "user u").
     * @param string      $whereProp   The column name.
     * @param mixed       $whereValue  The value to compare against.
     * @param string      $operator    SQL operator (=, >, <, IN, etc.)
     * @param string      $cond        Logical operator (AND/OR), default is AND.
     *
     * @return static
     */
    public function joinWhere(
        string $table,
        string $column,
        mixed $value = 'DBNULL',
        string $operator = '=',
        string $concat = 'AND'
    ): static {

        $table = $this->_validateTableName($table);

        $prefix = $this->prefix ?? '';
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
     * @param string      $whereJoin   The joined table name with optional alias (e.g. "user u").
     * @param string      $whereProp   The column name.
     * @param mixed       $whereValue  The value to compare against.
     * @param string      $operator    SQL operator (=, >, <, IN, etc.)
     *
     * @return static
     */
    public function joinOrWhere(
        string $table,
        string $column,
        mixed $value = 'DBNULL',
        string $operator = '='
    ): static {
        return $this->joinWhere($table, $column, $value, $operator, 'OR');
    }


    protected function _buildJoin(): void
    {
        foreach ($this->_pendingJoins as [$joinType, $joinTable, $joinCondition]) {
            $this->_query .= " {$joinType} JOIN {$joinTable} ON {$joinCondition}";

            // JOIN-WHERE
            $key = is_string($joinTable) ? trim($joinTable) : '';
            if (!empty($this->_joinWheres[$key])) {
                foreach ($this->_joinWheres[$key] as [$concat, $column, $operator, $value]) {
                    $col = str_contains($column, '.') ? $this->escapeTableAndColumn($column) : "`{$column}`";
                    $this->_query .= " {$concat} {$col}";
                    $this->_query .= ($value === null || $value === 'DBNULL') ? " {$operator} NULL" : " {$operator} ?";
                    if ($value !== null && $value !== 'DBNULL') {
                        $this->_bindParam($value);
                    }
                }
            }
        }
    }

    protected function escapeTableAndColumn(string $column): string
    {
        return '`' . str_replace('.', '`.`', $column) . '`';
    }

    /**
     * Converts a condition into a SQL string and adds necessary bindings.
     *
     * @param string $operator
     * @param mixed  $value
     * @return string
     */
    /**
     * Converts a condition into a SQL string and adds necessary bindings.
     *
     * @param string $operator SQL comparison operator.
     * @param mixed  $value    Value to compare.
     * @return string SQL fragment with placeholder(s).
     * @throws \InvalidArgumentException
     */
    protected function conditionToSql(string $operator, mixed $value): string
    {
        $operator = strtoupper(trim($operator));

        switch ($operator) {
            case 'IN':
            case 'NOT IN':
                if (!is_array($value)) {
                    throw new \InvalidArgumentException("Operator $operator requires an array as value.");
                }
                $placeholders = implode(', ', array_fill(0, count($value), '?'));
                $this->_bindParams($value);
                return " $operator ($placeholders)";

            case 'BETWEEN':
            case 'NOT BETWEEN':
                if (!is_array($value) || count($value) !== 2) {
                    throw new \InvalidArgumentException("Operator $operator requires an array with two values.");
                }
                $this->_bindParams($value);
                return " $operator ? AND ?";

            default:
                if (!in_array($operator, ['=', '!=', '<', '<=', '>', '>=', '<>', 'LIKE', 'NOT LIKE', 'IS', 'IS NOT'], true)) {
                    throw new \InvalidArgumentException("Unsupported SQL operator: $operator");
                }
                if ($value === null) {
                    return " $operator NULL";
                }

                $this->_bindParam($value);
                return " $operator ?";
        }
    }

    /**
     * Automatically expands array parameters into multiple placeholders.
     * Example: WHERE id IN (?) with [1, 2, 3] → WHERE id IN (?, ?, ?) and flat param list.
     */
    protected function expandArrayParams(): void
    {
        if (empty($this->_bindParams)) {
            return;
        }

        $newQuery = $this->_query;
        $newParams = [];

        foreach ($this->_bindParams as $param) {
            // Unterstützt neue Struktur ['value' => ..., 'type' => ...]
            $value = is_array($param) && array_key_exists('value', $param)
                ? $param['value']
                : $param;

            if (is_array($value)) {
                // Wenn value ein Array ist → mehrfacher Platzhalter nötig
                $placeholders = implode(', ', array_fill(0, count($value), '?'));
                $pos = strpos($newQuery, '?');

                if ($pos !== false) {
                    $newQuery = substr_replace($newQuery, $placeholders, $pos, 1);
                }

                foreach ($value as $val) {
                    $newParams[] = $val;
                }
            } else {
                $newParams[] = $value;
            }
        }

        $this->_query = $newQuery;
        $this->_bindParams = $newParams; // <-- wichtig: flach machen
    }

    /**
     * Sets or gets the current debug level.
     *
     * @param int|null $level 0 = silent, 1 = errors, 2 = full query + errors
     * @return int Current debug level
     */
    public function debug(?int $level = null): int
    {
        if ($level !== null) {
            $this->debugLevel = $level;
        }
        return $this->debugLevel;
    }

    protected function logException(\Throwable $e, ?string $context = null): void
    {
        if ($this->debugLevel < 1) {
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
        if ($this->debugLevel < 2) {
            return;
        }

        error_log("[Query] " . $sql);

        if (!empty($bindings)) {
            error_log("[Bindings] " . print_r($bindings, true));
        }

    }

    protected function handleException(\Throwable $e, string $context = ''): false
    {
        $this->_stmtError = $e->getMessage();
        $this->_stmtErrno = $e->getCode();

        if ($this->debugLevel >= 2 && !empty($this->_query)) {
            $this->logQuery($this->_query, $this->_bindParams ?? null);
        }

        $this->logException($e, $context);
        return false;
    }

    /**
     * Validates a table name to ensure it only contains allowed characters.
     *
     * @param string $tableName The table name to validate.
     * @return string The validated table name (can apply trim or other cleanups).
     * @throws \InvalidArgumentException If the table name is invalid.
     */
    protected function _validateTableName(string $tableName): string
    {
        // Entferne mögliche Backticks/Anführungszeichen am Anfang/Ende, falls diese vom Nutzer mitgegeben werden
        $tableName = trim($tableName, "`'\"");

        // Prüfe auf ungültige Zeichen
        // Erlaubt sind: Buchstaben (a-z, A-Z), Zahlen (0-9), Unterstrich (_)
        // Andere Zeichen wie Punkte (.), Leerzeichen, Anführungszeichen, Sonderzeichen etc. sind NICHT erlaubt.
        // Ein Punkt ist nur erlaubt, wenn es sich um "datenbank.tabelle" handelt (siehe unten)
        if (!preg_match('/^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+){0,2}$/', $tableName)) {
            throw new \InvalidArgumentException("Invalid characters found in table name: '{$tableName}'. Only alphanumeric characters and underscores are allowed, optionally with up to two dots for database.schema.table.");
        }

        // Zusätzliche Prüfung für "datenbank.tabelle" (max. 2 Punkte für db.schema.table oder db.table)
        $parts = explode('.', $tableName);
        if (count($parts) > 3) { // Max database.schema.table
            throw new \InvalidArgumentException("Invalid table name format: '{$tableName}'. Too many dots.");
        }
        foreach ($parts as $part) {
            if (!preg_match('/^[a-zA-Z0-9_]+$/', $part)) {
                // This check is actually redundant if the main regex is strong enough,
                // but good for explicit clarity if the main regex is simplified.
                throw new \InvalidArgumentException("Invalid characters in table name part: '{$part}'.");
            }
        }

        return $tableName;
    }

}