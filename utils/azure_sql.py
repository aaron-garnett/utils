import mssql_python
from mssql_python.exceptions import NotSupportedError, IntegrityError, DataError, ProgrammingError, OperationalError  # noqa: F401
from azure import identity
import struct
import pandas as pd
from typing import Any, List, Sequence
import time
import logging
from enum import Enum, auto

logger = logging.getLogger(__name__)


class AuthMethod(Enum):
    PASSWORDLESS = auto()
    USERNAME_PASSWORD = auto()


class AzureSqlConnection():
    """
    A class for connecting to Azure SQL Database and executing SQL queries. The class instance is intended to be used for the 
    duration of a program's execution and does not implicitly open or close the connection. Username and password are required 
    for password-based authentication, if not supplied, passwordless authentication will be attempted.
    """
    def __init__(self, server: str, database: str, schema: str, username: str | None = None, password: str | None = None, attempt_limit: int = 3, attempt_delay: int = 45) -> None:
        self.attempt_limit: int = attempt_limit
        self.attempt_delay: int = attempt_delay
        self.token: dict[Any, Any] | None = None
        self.attempt_count: int = 0
        self.connection: mssql_python.Connection | None = None
        self.initial_attempt_time: float | None = None
        self.connection_string: str = self.connection_string(server, database, username, password)
        self.auth_method: AuthMethod = self.auth_method(server, database, username, password)
        self.schema: str = schema

    def connection_string(self, server: str, database: str, username: str | None = None, password: str | None = None) -> str:
        """Constructs the connection string for Azure SQL Database based on the provided parameters and authentication method."""
        if username is None or password is None:
            logger.debug('Using passwordless authentication.')
            return f'SERVER=tcp:{server},1433;DATABASE={database};Encrypt=yes'
        else:
            logger.debug('Using password-based authentication.')
            return f'SERVER=tcp:{server},1433;DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no'

    def auth_method(self, server: str, database: str, username: str | None = None, password: str | None = None) -> AuthMethod:
        """Determines the authentication method to use based on the presence of username and password."""
        if username is None or password is None:
            return AuthMethod.PASSWORDLESS
        else:
            return AuthMethod.USERNAME_PASSWORD

    def connect(self) -> mssql_python.Connection:
        """Attempts to establish a connection to Azure SQL Database, with retry logic and support for both password-based and passwordless authentication."""
        logger.debug('Starting Azure SQL Database connection process...')
        if self.connection is not None:
            return self.connection
        if self.auth_method == AuthMethod.PASSWORDLESS:
            self.token = self.authenticate_passwordless() if self.token is None else self.token
        while self.connection is None and self.attempt_count <= self.attempt_limit:
            try:
                self.connection_attempt()
            except Exception as ex:
                self.connection_failure(ex)
            else:
                logger.info(f'Connection {self.connection} is succesful on attempt #{self.attempt_count}.')
                logger.debug(f'Initial attempt time: {self.initial_attempt_time}, current time: {time.time()}, elapsed time: {time.time() - self.initial_attempt_time:.0f} seconds.')
                return self.connection
        raise ConnectionError('Unable to connect to Azure SQL Database.')

    def authenticate_passwordless(self) -> dict[Any, Any]:
        """Performs passwordless authentication using Azure Identity and constructs the appropriate token structure for SQL connection."""
        logger.info('Attempting Azure passwordless authentication...')
        credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
        try:
            token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
        except Exception as ex:
            raise ConnectionError(f'Azure authentication failed: {ex}.')
        else:
            logger.info('Azure passwordless authentication succeeded.')
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
        sql_copt_ss_access_token = 1256  # This connection option is defined by microsoft in msodbcsql.h
        auth_token_attr = {sql_copt_ss_access_token: token_struct}
        return auth_token_attr

    def connection_attempt(self) -> None:
        """Attempts to establish a connection to Azure SQL Database, updating attempt count and timing information."""
        self.attempt_start_time = time.time()
        self.initial_attempt_time = self.attempt_start_time if self.initial_attempt_time is None else self.initial_attempt_time
        self.attempt_count += 1
        logger.info(f'Azure SQL Database connection attempt #{self.attempt_count}.')
        logger.debug(f'self.attempt_start_time: {self.attempt_start_time}, self.attempt_delay: {self.attempt_delay}, time.time(): {time.time()}')
        if self.auth_method == AuthMethod.PASSWORDLESS:
            self.connection = mssql_python.connect(self.connection_string, attrs_before=self.token)
        else:
            self.connection = mssql_python.connect(self.connection_string)
        return

    def connection_failure(self, exception: Exception) -> None:
        """Handles connection failure by logging the exception and implementing retry logic based on attempt count and timing."""
        exception_time = time.time()
        attempt_timeout_time = exception_time + self.attempt_delay
        logger.info(f'General error occured: {exception}. Retrying connection...')
        logger.debug(f'{exception_time - self.attempt_start_time:.0f} seconds elapsed since last attempt.')
        logger.debug(f'{exception_time - self.initial_attempt_time:.0f} seconds elapsed since initial attempt.')
        logger.debug(f'self.attempt_start_time: {self.attempt_start_time}, self.attempt_delay: {self.attempt_delay}, time.time(): {time.time()}')
        if attempt_timeout_time > time.time():
            wait_time = attempt_timeout_time - time.time()
            logger.info(f'Waiting {wait_time:.0f} seconds before re-attempting connection...')
            time.sleep(wait_time)
        return

    def basic_connectivity_test(self) -> None:
        """Performs a basic connectivity test by executing a simple SQL query to retrieve the current timestamp from the database."""
        logger.debug('Starting basic connectivity test...')
        if self.connection is None:
            raise ConnectionError('No active connection to test.')
        sql = "SELECT current_timestamp timestamp;"
        results = self.execute_sql(sql, return_results=True)
        for row in results:
            logger.debug(str(row[0]))
        return

    def direct_write(self, df: pd.DataFrame, table_name: str, create: bool=True, fast: bool=True, max_rows: int=10000) -> None:
        """Writes a pandas DataFrame directly to an Azure SQL Database table,
        with options for creating the table and controlling batch size for inserts."""
        table_name = f'{self.schema}.{table_name}'
        logger.debug(f'Starting direct write to table {table_name} (create={create}, fast={fast}, max_rows={max_rows})...')
        if self.connection is None:
            raise ConnectionError('No active connection to write data.')
        columns: list[str] = df.columns.tolist()
        columns_string: str = ', '.join([f'"{column}"' for column in columns])
        insert_placeholders: str = ', '.join(map(lambda _: '?', columns))
        df = df.astype('string')
        data: Sequence[Sequence[Any]] = list(df.itertuples(index=False, name=None))
        if create:
            sql = f"DROP TABLE IF EXISTS {table_name};"
            self.execute_sql(sql)

            sql: str = f"CREATE TABLE {table_name} (\n"
            for column in columns:
                sql += f'"{column}" varchar(255),\n'
            sql += ");"
            self.execute_sql(sql)

        for start_row in range(0, len(data), max_rows):
            logger.info(f'Inserting rows {start_row + 1} to {min(start_row + max_rows, len(data))} ...')
            end_row = start_row + max_rows
            batch_data = data[start_row:end_row]
            sql = f"INSERT INTO {table_name} ({columns_string}) values ({insert_placeholders})"
            self.execute_sql(sql, data=batch_data)
        return

    def direct_read(self, table_name: str) -> list[dict[str, Any]]:
        """Reads all data from a specified Azure SQL Database table and returns it as a 
        list of dictionaries, where each dictionary represents a row with column names as keys."""
        table_name = f'{self.schema}.{table_name}'
        logger.debug(f'Starting direct read from table {table_name}...')
        if self.connection is None:
            raise ConnectionError('No active connection to write data.')
        rows: list[dict[str, Any]] = []
        sql = f"SELECT * FROM {table_name}"
        results = self.execute_sql(sql, return_results=True)
        columns = [item[0] for item in results[0].cursor_description]
        for row in results:
            row_dict: dict[str, Any] = {}
            for i, column_name in enumerate(columns):
                row_dict[column_name] = row[i]
            rows.append(row_dict)
        return rows

    def execute_sql(self, sql: str, data: list | None = None, return_results: bool = False) -> List[mssql_python.cursor.Row]:
        """Executes a SQL query against the Azure SQL Database,
        with support for parameterized queries and optional result retrieval."""
        if self.connection is None:
            raise ConnectionError('No active connection to execute SQL.')
        cursor = self.connection.cursor()
        try:
            if data is not None:
                cursor.executemany(sql, data)
                logger.debug(f'Executed SQL: {sql} with {len(data)} data rows.')
            else:
                cursor.execute(sql)
                logger.debug(f'Executed SQL: {sql}')
        except Exception as ex:
            cursor.rollback()
            logger.error(f'Error executing SQL: {ex}.')
            raise
        else:
            logger.debug(f'{cursor.rowcount} rows affected.')
            if return_results:
                results = cursor.fetchall()
                cursor.commit()
                return results
            else:
                cursor.commit()
                return []
