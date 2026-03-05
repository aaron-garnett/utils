import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import sqlparse
import pandas as pd
import logging
import re
from enum import Enum, auto
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def _validate_identifier(name: str) -> str:
    """Validate that a SQL identifier (table, column, schema) contains only safe characters."""
    if not re.match(r'^[\w\s./]+$', name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


logger = logging.getLogger(__name__)


class AuthMethod(Enum):
    KEY_PAIR = auto()
    USERNAME_PASSWORD = auto()


class SnowflakeConnection:
    """
    A class for connecting to a Snowflake database and executing SQL queries. Mirrors the
    interface of AzureSqlConnection, MySqlConnection, and SqliteConnection.

    Authentication: provide either private_key_path (for key-pair auth) or password
    (for username/password auth). Key-pair auth is preferred for service accounts.
    """

    def __init__(
        self,
        account: str,
        user: str,
        database: str,
        schema: str | None = None,
        warehouse: str | None = None,
        private_key_path: str | None = None,
        password: str | None = None,
    ) -> None:
        self.account = account
        self.user = user
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.private_key_path = private_key_path
        self.password = password
        self.auth_method: AuthMethod = self._resolve_auth_method()
        self._connection: snowflake.connector.SnowflakeConnection | None = None

    def _resolve_auth_method(self) -> AuthMethod:
        if self.private_key_path is not None:
            return AuthMethod.KEY_PAIR
        elif self.password is not None:
            return AuthMethod.USERNAME_PASSWORD
        else:
            raise ValueError("Either private_key_path or password must be provided.")

    def _load_private_key_bytes(self) -> bytes:
        with open(self.private_key_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

    def connect(self) -> snowflake.connector.SnowflakeConnection:
        logger.debug('Starting Snowflake connection process...')
        if self._connection is not None:
            return self._connection
        params = {
            'user': self.user,
            'account': self.account,
            'database': self.database,
        }
        if self.schema:
            params['schema'] = self.schema
        if self.warehouse:
            params['warehouse'] = self.warehouse
        if self.auth_method == AuthMethod.KEY_PAIR:
            logger.debug('Using key-pair authentication.')
            params['private_key'] = self._load_private_key_bytes()
        else:
            logger.debug('Using username/password authentication.')
            params['password'] = self.password
        self._connection = snowflake.connector.connect(**params)
        logger.info(f'Connected to Snowflake: {self.database}.{self.schema}')
        return self._connection

    def basic_connectivity_test(self) -> None:
        logger.debug('Starting basic connectivity test...')
        if self._connection is None:
            raise ConnectionError('No active connection to test.')
        sql = 'SELECT current_timestamp as timestamp;'
        results, _ = self.execute_sql(sql, return_results=True)
        for row in results:
            logger.debug(str(row[0]))

    def read_table(self, table_name: str) -> pd.DataFrame:
        _validate_identifier(table_name)
        logger.debug(f'Reading table {table_name}...')
        if self._connection is None:
            raise ConnectionError('No active connection to read data.')
        cur = self._connection.cursor()
        cur.execute('SELECT * FROM IDENTIFIER(%s)', (table_name,))
        return cur.fetch_pandas_all()

    def write_table(self, df: pd.DataFrame, table_name: str, create: bool = True) -> None:
        _validate_identifier(table_name)
        logger.debug(f'Writing table {table_name} (create={create})...')
        if self._connection is None:
            raise ConnectionError('No active connection to write data.')
        if create:
            self.drop_table(table_name)
            self.create_table(table_name, df.columns.tolist())
        if self.schema:
            cur = self._connection.cursor()
            cur.execute('USE SCHEMA IDENTIFIER(%s)', (self.schema,))
        write_pandas(self._connection, df, table_name, quote_identifiers=False)
        logger.info(f'Wrote {len(df)} rows to {table_name}.')

    def execute_sql(self, sql: str, data: list | None = None, return_results: bool = False) -> tuple[list | pd.DataFrame, list]:
        """Execute a SQL statement. If return_results=True, returns (rows, description) like other connectors,
        OR pass fetch_pandas=True on the cursor yourself for large result sets."""
        if self._connection is None:
            raise ConnectionError('No active connection to execute SQL.')
        cur = self._connection.cursor()
        try:
            if data is not None:
                logger.debug(f'Executing SQL: {sql} with {len(data)} data rows.')
                cur.executemany(sql, data)
            else:
                logger.debug(f'Executing SQL: {sql}')
                cur.execute(sql)
        except Exception as ex:
            logger.error(f'Error executing SQL: {ex}.')
            raise
        else:
            if return_results:
                rows = cur.fetchall()
                description = list(cur.description) if cur.description else []
                return list(rows), description
            return [], []

    def execute_sql_returning_df(self, sql: str) -> pd.DataFrame:
        """Execute a SELECT statement and return results as a DataFrame (Snowflake-optimised path)."""
        if self._connection is None:
            raise ConnectionError('No active connection to execute SQL.')
        cur = self._connection.cursor()
        logger.debug(f'Executing SQL: {sql}')
        cur.execute(sql)
        return cur.fetch_pandas_all()

    def execute_sql_script_file(self, sql_script_path: str) -> None:
        """Execute all statements in a .sql script file."""
        with open(sql_script_path, 'r') as f:
            script = f.read()
        for statement in sqlparse.split(script):
            executable = sqlparse.format(statement, strip_comments=True).strip()
            if executable:
                self.execute_sql(executable)

    def drop_table(self, table_name: str) -> None:
        _validate_identifier(table_name)
        logger.debug(f'Dropping table {table_name}...')
        self.execute_sql(f'DROP TABLE IF EXISTS {table_name};')

    def create_table(self, table_name: str, columns: list[str]) -> None:
        _validate_identifier(table_name)
        for col in columns:
            _validate_identifier(col)
        logger.debug(f'Creating table {table_name}...')
        cols_sql = ', '.join([f'{col} TEXT NULL' for col in columns])
        sql = f'CREATE TABLE {table_name} ({cols_sql});'
        self.execute_sql(sql)

    def get_columns(self) -> pd.DataFrame:
        """Return a DataFrame with metadata for all columns in the current schema."""
        schema_filter = f"= '{self.schema}'" if self.schema else 'IS NOT NULL'
        sql = f"""
            SELECT TABLE_NAME as "table", COLUMN_NAME as column_name,
                   DATA_TYPE as datatype, NULL as "key"
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA {schema_filter}
            ORDER BY TABLE_NAME, ORDINAL_POSITION;
        """
        return self.execute_sql_returning_df(sql)

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.debug('Snowflake connection closed.')

    def __repr__(self) -> str:
        return f'SnowflakeConnection({self.database}.{self.schema})'
