# utils

Reusable infrastructure utilities for database connectivity and data I/O. Supports Azure SQL, MySQL (via SSH tunnel), SQLite, Snowflake, Google BigQuery, and Google Sheets.

---

## Installation

```bash
uv pip install -e .
```

For Snowflake support:
```bash
uv pip install -e ".[snowflake]"
```

For Google BigQuery support:
```bash
uv pip install -e ".[bigquery]"
```

---

## Common SQL Interface

All database connector classes (`AzureSqlConnection`, `MySqlConnection`, `SqliteConnection`, `SnowflakeConnection`, `BigQueryConnection`) share a consistent interface, making backends interchangeable with minimal adaptation:

| Method | Description |
|---|---|
| `connect()` | Opens the connection. Idempotent — safe to call multiple times. |
| `read_table(table_name)` | `SELECT *` from a table. Returns a `pd.DataFrame`. |
| `write_table(df, table_name, create=True)` | Writes a DataFrame to a table. When `create=True` (default), drops and recreates the table first. All columns written as `VARCHAR(255)` / `TEXT`. |
| `execute_sql(sql, data=None, return_results=False)` | Executes a SQL string. Pass `data` as a list of tuples for parameterized batch execution. Returns `(rows, description)` when `return_results=True`. |
| `drop_table(table_name)` | `DROP TABLE IF EXISTS`. |
| `create_table(table_name, columns)` | Creates a table with all columns as `VARCHAR(255)` / `TEXT`. |
| `basic_connectivity_test()` | Executes a trivial query to verify the connection is live. |
| `get_columns()` | Returns a `pd.DataFrame` of all columns in the schema with `table`, `column_name`, `datatype`, and `key` fields. |
| `close()` | Closes the connection (and SSH tunnel, where applicable). |

> **BigQuery note:** `BigQueryConnection.execute_sql()` returns a `pd.DataFrame` directly (not a `(rows, description)` tuple) because BigQuery's API is job-based rather than cursor-based.

All identifier names (tables, columns, schemas) are validated against `r'^[\w\s.]+$'` before use in SQL strings, preventing injection through identifiers.

---

## Modules

### `utils.azure_sql` — Azure SQL Database

Class-based interface for Azure SQL Database using `mssql-python` and optionally SQLAlchemy.

#### `AzureSqlConnection`

```python
from utils.azure_sql import AzureSqlConnection

conn = AzureSqlConnection(server, database, schema, username=None, password=None)
conn.connect()
df = conn.read_table("my_table")
conn.write_table(df, "my_table")
```

**Constructor parameters**

| Parameter | Type | Description |
|---|---|---|
| `server` | `str` | Azure SQL server hostname |
| `database` | `str` | Database name |
| `schema` | `str` | Default schema (e.g. `dbo`) |
| `username` | `str \| None` | SQL username (optional) |
| `password` | `str \| None` | SQL password (optional) |
| `attempt_limit` | `int` | Max connection retry attempts (default: 3) |
| `attempt_delay` | `int` | Seconds to wait between retries (default: 45) |

**Additional methods**

| Method | Description |
|---|---|
| `connect_sqlalchemy()` | Opens and returns a SQLAlchemy connection (for pandas/ORM workflows). |
| `write_table(df, table_name, create=True, fast=True, max_rows=10000, columns=None)` | Supports configurable batch size and optional explicit column list. |

**Auth flow**

- Username + password supplied → SQL authentication via connection string.
- Neither supplied → `DefaultAzureCredential` token flow (supports Managed Identity, CLI login, browser interactive).
- Connection failures trigger automatic retries up to `attempt_limit` with `attempt_delay`-second waits.

---

### `utils.my_sql` — MySQL over SSH Tunnel

Class-based interface for MySQL, with optional SSH tunnel support. Backward-compatible module-level functions are retained as deprecated shims.

#### `MySqlConnection`

```python
from utils.my_sql import MySqlConnection

conn = MySqlConnection(
    host="mysql.example.com",
    user="db_user",
    password="...",
    database="my_database",
    ssh_host="bastion.example.com",
    ssh_user="ssh_user",
    ssh_pw="...",
)
conn.connect()
df = conn.read_table("my_table")
conn.write_table(df, "my_table")
conn.close()
```

**Constructor parameters**

| Parameter | Type | Description |
|---|---|---|
| `host` | `str` | MySQL server hostname |
| `user` | `str` | MySQL username |
| `password` | `str` | MySQL password |
| `database` | `str \| None` | Database name (optional) |
| `ssh_host` | `str \| None` | SSH bastion hostname (omit for direct connection) |
| `ssh_user` | `str \| None` | SSH username |
| `ssh_pw` | `str \| None` | SSH password |
| `ssh_port` | `int` | SSH port (default: 22) |
| `mysql_port` | `int` | MySQL port (default: 3306) |

The SSH tunnel is started once on `connect()` and torn down on `close()`. This avoids the per-operation tunnel start/stop overhead of the legacy shim functions.

**Deprecated shims** (kept for backward compatibility — migrate to `MySqlConnection`):

```python
# Old pattern — deprecated
tunnel = my_sql.ssh_connect(ssh_host, ssh_user, ssh_pw, host)
conn = my_sql.mysql_connect(tunnel, user, password, database)
df = my_sql.get_table(tunnel, conn, "my_table")
my_sql.write_table(tunnel, conn, "my_table", df)
```

---

## Migration Guide — MySQL shims → `MySqlConnection`

The module-level functions `ssh_connect`, `mysql_connect`, `get_table`, and `write_table` are deprecated and will be removed in a future release. Replace them with the `MySqlConnection` class.

### Why migrate?

- The old shims open and close the SSH tunnel on every call, adding latency on every operation.
- `MySqlConnection` opens the tunnel once on `connect()` and tears it down on `close()`, which is more efficient and less error-prone.
- `MySqlConnection` shares a consistent interface with `AzureSqlConnection`, `SqliteConnection`, `SnowflakeConnection`, and `BigQueryConnection`, making connectors interchangeable.

### Before (deprecated)

```python
from utils import my_sql

tunnel = my_sql.ssh_connect(ssh_host, ssh_user, ssh_pw, host)
tunnel.start()
conn = my_sql.mysql_connect(tunnel, user, password, database)

df = my_sql.get_table(tunnel, conn, "my_table")
my_sql.write_table(tunnel, conn, "my_table", df)

conn.close()
tunnel.stop()
```

### After (`MySqlConnection`)

```python
from utils.my_sql import MySqlConnection

conn = MySqlConnection(
    host=host,
    user=user,
    password=password,
    database=database,
    ssh_host=ssh_host,
    ssh_user=ssh_user,
    ssh_pw=ssh_pw,
)
conn.connect()

df = conn.read_table("my_table")
conn.write_table(df, "my_table")

conn.close()
```

### Mapping: old function → new method

| Deprecated call | `MySqlConnection` equivalent |
|---|---|
| `ssh_connect(ssh_host, ssh_user, ssh_pw, host)` | Pass these as constructor arguments; tunnel starts on `connect()`. |
| `mysql_connect(tunnel, user, password, database)` | `MySqlConnection(...).connect()` |
| `get_table(tunnel, conn, "my_table")` | `conn.read_table("my_table")` |
| `write_table(tunnel, conn, "my_table", df)` | `conn.write_table(df, "my_table")` |

---

### `utils.sqlite` — SQLite

Class-based interface for SQLite databases.

#### `SqliteConnection`

```python
from utils.sqlite import SqliteConnection

conn = SqliteConnection("/path/to/database.db")
conn.connect()
df = conn.read_table("my_table")
conn.write_table(df, "my_table")
conn.close()
```

**Constructor parameters**

| Parameter | Type | Description |
|---|---|---|
| `file_path` | `str` | Path to the SQLite database file. Created if it does not exist. |

**Notes**

- Uses `?` parameter placeholders (DB-API 2.0 `qmark` style), compatible with `database_mining` utility functions.
- `execute_sql` rolls back on error and commits on success.
- `get_columns()` introspects `sqlite_master` to parse schema metadata.

---

### `utils.snowflake` — Snowflake

Class-based interface for Snowflake.

#### `SnowflakeConnection`

```python
from utils.snowflake import SnowflakeConnection

# Key-pair authentication (preferred for service accounts)
conn = SnowflakeConnection(
    account="abc12345.us-east-1",
    user="svc_account",
    database="MY_DB",
    schema="MY_SCHEMA",
    warehouse="COMPUTE_WH",
    private_key_path="/path/to/rsa_key.p8",
)

# Username/password authentication
conn = SnowflakeConnection(
    account="abc12345.us-east-1",
    user="my_user",
    database="MY_DB",
    schema="MY_SCHEMA",
    password="...",
)

conn.connect()
df = conn.read_table("my_table")
conn.write_table(df, "my_table")
conn.close()
```

**Constructor parameters**

| Parameter | Type | Description |
|---|---|---|
| `account` | `str` | Snowflake account identifier (e.g. `abc12345.us-east-1`) |
| `user` | `str` | Snowflake username |
| `database` | `str` | Database name |
| `schema` | `str \| None` | Schema name (optional) |
| `warehouse` | `str \| None` | Warehouse name (optional) |
| `private_key_path` | `str \| None` | Path to `.p8` private key file (key-pair auth) |
| `password` | `str \| None` | Password (username/password auth) |

Exactly one of `private_key_path` or `password` must be provided.

**Additional methods**

| Method | Description |
|---|---|
| `execute_sql_returning_df(sql)` | Executes a SELECT and returns results as a DataFrame via Snowflake's native `fetch_pandas_all()` (more efficient than `execute_sql` for large result sets). |
| `execute_sql_script_file(path)` | Reads a `.sql` file, splits it into individual statements with `sqlparse`, and executes each one. |

**Required extras:** `uv pip install -e ".[snowflake]"`

---

### `utils.google_cloud` — Google Cloud BigQuery

Class-based interface for Google Cloud BigQuery.

#### `BigQueryConnection`

```python
from utils.google_cloud import BigQueryConnection

# Service account authentication
conn = BigQueryConnection(
    project_id="my-gcp-project",
    dataset_id="my_dataset",
    credentials_path="/path/to/service_account.json",
)

# Application Default Credentials (ADC)
conn = BigQueryConnection(
    project_id="my-gcp-project",
    dataset_id="my_dataset",
)

conn.connect()
df = conn.read_table("my_table")
conn.write_table(df, "my_table")
conn.close()
```

**Constructor parameters**

| Parameter | Type | Description |
|---|---|---|
| `project_id` | `str` | GCP project ID |
| `dataset_id` | `str` | BigQuery dataset (analogous to schema) |
| `credentials_path` | `str \| None` | Path to service account JSON key file. Omit to use Application Default Credentials. |

**Interface notes**

- `execute_sql(sql)` returns a `pd.DataFrame` (not a `(rows, description)` tuple) — BigQuery's API is job-based, not cursor-based.
- `write_table(..., create=True)` uses `WRITE_TRUNCATE` (replace); `create=False` uses `WRITE_APPEND`.
- Tables are automatically fully qualified as `project.dataset.table`. Pass a dotted name to override.
- DDL (`create_table`, `drop_table`) is executed via `execute_sql` using standard BigQuery SQL syntax.

**Required extras:** `uv pip install -e ".[bigquery]"`

---

### `utils.database_mining` — Schema Exploration and Data Comparison

DB-agnostic analytical utilities. Works with any DB-API 2.0 connection that uses `?` placeholders (SQLite, Azure SQL via `mssql_python`).

> **Note:** SQLite connectivity has been moved to `utils.sqlite.SqliteConnection`. Use that class to open connections; pass the `connection` attribute to `database_mining` functions.

#### Functions

| Function | Signature | Description |
|---|---|---|
| `get_column_values` | `(connection, table, column, value=None) -> list` | Fetches all values from a column, with optional equality filter. Compatible with SQLite and Azure SQL (`?` placeholder style). |
| `find_value` | `(connection, schema_columns, datatype, value)` | Searches every column of a given datatype for a specific value. Prints matches. |
| `find_primary_key` | `(connection, foreign_key_table, foreign_key_column, schema_columns, keys_only=False, strict_type=True) -> pd.DataFrame` | For a given foreign key column, scans the schema to find candidate primary key columns by value overlap. Returns a DataFrame ranked by `percent_match`. |
| `compare_dataframes` | `(df1, df2) -> (int, int)` | Outer-merges two DataFrames on all columns. Returns `(same_rows, different_rows)` and prints a sample of non-matching rows. |
| `compare_tables` | `(dataframe1, dataframe2) -> dict` | Deep comparison of two DataFrames: reports column overlap, row count overlap (by index), per-column value cardinality, and per-column match rates. Returns a summary dict with a `df` key containing per-column match rates. |

**Typical usage with SQLite:**

```python
from utils.sqlite import SqliteConnection
from utils import database_mining

db = SqliteConnection("/path/to/my.db")
db.connect()
schema = db.get_columns()
database_mining.find_value(db.connection, schema, "TEXT", "some_value")
```

---

### `utils.google_sheets` — Google Sheets I/O

Thin wrappers around `pygsheets` for reading and writing Google Sheets via a service account.

#### Functions

| Function | Signature | Description |
|---|---|---|
| `list_all_google_sheets` | `(connection) -> list[str]` | Lists all spreadsheets accessible to the service account. Logs title, ID, and last-updated timestamp for each. |
| `purge_all_google_sheets` | `(connection, sheet_ids_to_keep)` | Deletes all accessible spreadsheets except those in `sheet_ids_to_keep`. Accepts a single ID or a list. |
| `write_df_to_google_sheet` | `(google_service_acct_file, sheet_id, worksheet_name, dataframe, clear_existing=True, resize_existing=True, field_leading_character="'")` | Writes a DataFrame to a named worksheet. Optionally clears existing content, resizes the sheet to fit the data, and prepends a leading character to all values (default `'`) to prevent Google Sheets from misinterpreting values as formulas or dates. Raises `ValueError` if the DataFrame exceeds 10 million cells. |

---

## Connector Compatibility

| Capability | Azure SQL | MySQL | SQLite | Snowflake | BigQuery |
|---|---|---|---|---|---|
| Style | Class | Class | Class | Class | Class |
| `read_table` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `write_table` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `execute_sql` | ✅ `(rows, desc)` | ✅ `(rows, desc)` | ✅ `(rows, desc)` | ✅ `(rows, desc)` | ✅ `DataFrame` |
| `get_columns` | ✅ INFORMATION_SCHEMA | ✅ INFORMATION_SCHEMA | ✅ sqlite_master | ✅ INFORMATION_SCHEMA | ✅ INFORMATION_SCHEMA |
| Schema introspection | ✅ | ✅ | ✅ | ✅ | ✅ |
| Batch insert | ✅ configurable | ✅ executemany | ✅ executemany | via `write_pandas` | via load job |
| Retry logic | ✅ configurable | ❌ | ❌ | ❌ | ❌ |
| SQL script files | ❌ | ❌ | ❌ | ✅ | ❌ |
| `database_mining` compatible | ✅ (`?` placeholders) | ❌ (`%s` placeholders) | ✅ (`?` placeholders) | ❌ | ❌ |

---

## Future Development Opportunities

### Short-term

- **Connection context managers** — `with AzureSqlConnection(...) as conn:` pattern to ensure cleanup on exit, reducing the risk of leaked connections.
- **MySQL `get_table` / `write_table` shim removal** — migrate remaining callers to `MySqlConnection` then remove the deprecated module-level functions.
- **Deprecated shim removal** — migrate remaining `my_sql` module-level function callers to `MySqlConnection`, then remove the deprecated shims.

### Medium-term

- **Type-aware writes** — infer SQL column types from DataFrame dtypes instead of defaulting everything to `VARCHAR(255)` / `TEXT`.
- **Google Sheets read support** — a `read_df_from_google_sheet` counterpart to `write_df_to_google_sheet`.
- **Retry logic** — extend the configurable retry pattern from `AzureSqlConnection` to `MySqlConnection` and `SnowflakeConnection`.
- **`database_mining` MySQL compatibility** — make `get_column_values` and related functions work with `%s` placeholder style to support MySQL connections.

### Long-term

- **Migration utilities** — tools to copy tables between backends (e.g., MySQL → Azure SQL) using the existing `read_table` / `write_table` primitives.
- **Schema diffing across live connections** — extend `compare_tables` to operate directly against database connections rather than only in-memory DataFrames.
- **Credential management** — centralized secret/credential loading (e.g., from Azure Key Vault or environment files) rather than per-module credential passing.
- **Async support** — async variants of the connection and query methods for use in async pipelines.
