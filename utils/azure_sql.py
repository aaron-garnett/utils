import mssql_python
from mssql_python.exceptions import NotSupportedError, IntegrityError, DataError, ProgrammingError, OperationalError  # noqa: F401
from azure import identity
import struct
import pandas as pd
from typing import Any, Sequence
import time
import logging
from sqlalchemy import create_engine
import sqlalchemy
from enum import Enum, auto
from sqlalchemy.orm import declarative_base

Base = declarative_base()

logger = logging.getLogger(__name__)


class AuthMethod(Enum):
    PASSWORDLESS = auto()
    USERNAME_PASSWORD = auto()


class AzureSqlConnection():
    """
    A class for connecting to Azure SQL Database and executing SQL queries. The class instance is intended to be used for the
    duration of a program's execution and does not implicitly open or close the connection. Username and password are required
    for password-based authentication; if not supplied, passwordless authentication will be attempted.
    """
    def __init__(self, server: str, database: str, schema: str, username: str | None = None, password: str | None = None, attempt_limit: int = 3, attempt_delay: int = 45, **kwargs: dict[str,Any]) -> None:
        self.server: str = server or kwargs.get('server')
        self.database: str = database or kwargs.get('database')
        self.schema: str | None = schema or kwargs.get('schema')
        self.username: str | None = username or kwargs.get('username')
        self.password: str | None = password or kwargs.get('password')
        self.attempt_limit: int = attempt_limit or kwargs.get('attempt_limit')
        self.attempt_delay: int = attempt_delay or kwargs.get('attempt_delay')
        self.token: dict[Any, Any] | None = None
        self.attempt_count: int = 0
        self.mssql_connection: mssql_python.Connection | None = None
        self.sqlalchemy_connection: sqlalchemy.engine.base.Connection | None = None
        self.initial_attempt_time: float | None = None
        self.connection_string: str = self.connection_string()
        self.auth_method: AuthMethod = self.auth_method()

    def connect(self) -> mssql_python.Connection:
        logger.debug('Starting Azure SQL Database connection process...')
        if self.mssql_connection is not None:
            return self.mssql_connection
        if self.auth_method == AuthMethod.PASSWORDLESS:
            self.token = self.authenticate_passwordless() if self.token is None else self.token
        while self.mssql_connection is None and self.attempt_count <= self.attempt_limit:
            try:
                self.connection_attempt()
            except Exception as ex:
                self.connection_failure(ex)
            else:
                logger.info(f'Connection {self.mssql_connection} is succesful on attempt #{self.attempt_count}.')
                logger.debug(f'Initial attempt time: {self.initial_attempt_time}, current time: {time.time()}, elapsed time: {time.time() - self.initial_attempt_time:.0f} seconds.')
                return self.mssql_connection
        raise ConnectionError('Unable to connect to Azure SQL Database.')

    def connect_sqlalchemy(self) -> sqlalchemy.engine.base.Connection:
        if self.username is None or self.password is None:
            logger.debug('Using passwordless authentication.')
            connection_string = f'mssql+pyodbc://@{self.server}:1433/{self.database}?driver=ODBC+Driver+18+for+SQL+Server'
        else:
            logger.debug('Using password-based authentication.')
            connection_string = f'mssql+pyodbc://{self.username}:{self.password}@{self.server}:1433/{self.database}?driver=ODBC+Driver+18+for+SQL+Server'
        engine = create_engine(connection_string)
        connection = engine.connect()
        return connection

    def connection_string(self) -> str:
        if self.username is None or self.password is None:
            logger.debug('Using passwordless authentication.')
            connection_string = f'SERVER=tcp:{self.server},1433;DATABASE={self.database};Encrypt=yes'
            logger.debug(f'Connection string: {connection_string}')
        else:
            logger.debug('Using password-based authentication.')
            connection_string = f'SERVER=tcp:{self.server},1433;DATABASE={self.database};UID={self.username};PWD={self.password};Encrypt=yes;TrustServerCertificate=no'
            logger.debug(f'Connection string: {connection_string.replace(self.password, "********")}')
        return connection_string

    def auth_method(self) -> AuthMethod:
        if self.username is None or self.password is None:
            return AuthMethod.PASSWORDLESS
        else:
            return AuthMethod.USERNAME_PASSWORD

    def authenticate_passwordless(self) -> dict[Any, Any]:
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
        self.attempt_start_time = time.time()
        self.initial_attempt_time = self.attempt_start_time if self.initial_attempt_time is None else self.initial_attempt_time
        self.attempt_count += 1
        logger.info(f'Azure SQL Database connection attempt #{self.attempt_count}.')
        logger.debug(f'self.attempt_start_time: {self.attempt_start_time}, self.attempt_delay: {self.attempt_delay}, time.time(): {time.time()}')
        if self.auth_method == AuthMethod.PASSWORDLESS:
            self.mssql_connection = mssql_python.connect(self.connection_string, attrs_before=self.token)
        else:
            self.mssql_connection = mssql_python.connect(self.connection_string)
        return

    def connection_failure(self, exception: Exception) -> None:
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
        logger.debug('Starting basic connectivity test...')
        if self.mssql_connection is None:
            raise ConnectionError('No active connection to test.')
        sql = "SELECT current_timestamp timestamp;"
        results, description = self.execute_sql(sql, return_results=True)
        for row in results:
            logger.debug(str(row[0]))
        return

    def write_table(self, df: pd.DataFrame, table_name: str, create: bool=True, fast: bool=True, max_rows: int=10000, columns: list[str] | None = None) -> None:
        table_name = f'{self.schema}.{table_name}'
        logger.debug(f'Starting direct write to table {table_name} (create={create}, fast={fast}, max_rows={max_rows})...')
        if self.mssql_connection is None:
            raise ConnectionError('No active connection to write data.')
        df_columns: list[str] = df.columns.tolist()
        columns_string: str = ', '.join([f'"{column}"' for column in df_columns])
        insert_placeholders: str = ', '.join(map(lambda _: '?', df_columns))
        df = df.astype('string').fillna('')
        data: Sequence[Sequence[Any]] = list(df.itertuples(index=False, name=None))
        if create:
            self.drop_table(table_name)
            self.create_table(table_name, columns or df_columns)
        for start_row in range(0, len(data), max_rows):
            logger.info(f'Inserting rows {start_row + 1} to {min(start_row + max_rows, len(data))} ...')
            end_row = start_row + max_rows
            batch_data = data[start_row:end_row]
            sql = f"INSERT INTO {table_name} ({columns_string}) values ({insert_placeholders})"
            self.execute_sql(sql, data=batch_data)
        return

    def read_table(self, table_name: str) -> pd.DataFrame:
        table_name = f'{self.schema}.{table_name}'
        logger.debug(f'Starting direct read from table {table_name}...')
        if self.mssql_connection is None:
            raise ConnectionError('No active connection to write data.')
        rows: list[dict[str, Any]] = []
        sql = f"SELECT * FROM {table_name}"
        results, description = self.execute_sql(sql, return_results=True)
        columns = [item[0] for item in description]
        for row in results:
            row_dict: dict[str, Any] = {}
            for i, column_name in enumerate(columns):
                row_dict[column_name] = row[i]
            rows.append(row_dict)
        df = pd.DataFrame(rows)
        return df

    def execute_sql(self, sql: str, data: list | None = None, return_results: bool = False) -> tuple[list[mssql_python.cursor.Row], list[tuple[str, int]]]:
        if self.mssql_connection is None:
            raise ConnectionError('No active connection to execute SQL.')
        cursor = self.mssql_connection.cursor()
        try:
            if data is not None:
                logger.debug(f'Executing SQL: {sql} with {len(data)} data rows.')
                cursor.executemany(sql, data)
            else:
                logger.debug(f'Executing SQL: {sql}')
                cursor.execute(sql)
        except Exception as ex:
            cursor.rollback()
            logger.error(f'Error executing SQL: {ex}.')
            raise
        else:
            logger.debug(f'{cursor.rowcount} rows affected.')
            if return_results:
                results = cursor.fetchall()
                description = cursor.description
                cursor.commit()
                return results, description
            else:
                cursor.commit()
                return [], []

    def drop_table(self, table_name: str) -> None:
        logger.debug(f'Starting table drop for {table_name}...')
        sql = f"DROP TABLE IF EXISTS {table_name};"
        self.execute_sql(sql)
        return

    def create_table(self, table_name: str, columns: list[tuple[str, str]]) -> None:
        logger.debug(f'Starting table creation for {table_name}...')
        sql: str = f"CREATE TABLE {table_name} (\n"
        for column in columns:
            sql += f'"{column}" varchar(255),\n'
        sql += ");"
        self.execute_sql(sql)
        return
