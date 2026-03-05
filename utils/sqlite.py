import sqlite3
import pandas as pd
import re
import logging
from typing import Any, Sequence


def _validate_identifier(name: str) -> str:
    """Validate that a SQL identifier (table, column) contains only safe characters."""
    if not re.match(r'^[\w\s.]+$', name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


logger = logging.getLogger(__name__)


class SqliteConnection:
    """
    A class for connecting to a SQLite database and executing SQL queries. Mirrors the
    interface of AzureSqlConnection and MySqlConnection for cross-connector compatibility.
    """

    def __init__(self, file_path: str) -> None:
        self.file_path: str = file_path
        self.connection: sqlite3.Connection | None = None

    def connect(self) -> sqlite3.Connection:
        logger.debug(f'Connecting to SQLite database at {self.file_path}...')
        if self.connection is not None:
            return self.connection
        self.connection = sqlite3.connect(self.file_path)
        logger.info(f'Connected to SQLite database at {self.file_path}.')
        return self.connection

    def basic_connectivity_test(self) -> None:
        logger.debug('Starting basic connectivity test...')
        if self.connection is None:
            raise ConnectionError('No active connection to test.')
        sql = "SELECT datetime('now') as timestamp;"
        results, _ = self.execute_sql(sql, return_results=True)
        for row in results:
            logger.debug(str(row[0]))

    def read_table(self, table_name: str) -> pd.DataFrame:
        _validate_identifier(table_name)
        logger.debug(f'Reading table {table_name}...')
        if self.connection is None:
            raise ConnectionError('No active connection to read data.')
        sql = f'SELECT * FROM "{table_name}"'
        results, description = self.execute_sql(sql, return_results=True)
        columns = [item[0] for item in description]
        return pd.DataFrame(results, columns=columns)

    def write_table(self, df: pd.DataFrame, table_name: str, create: bool = True, max_rows: int = 10000) -> None:
        _validate_identifier(table_name)
        logger.debug(f'Writing table {table_name} (create={create}, max_rows={max_rows})...')
        if self.connection is None:
            raise ConnectionError('No active connection to write data.')
        df_columns: list[str] = df.columns.tolist()
        columns_string: str = ', '.join([f'"{col}"' for col in df_columns])
        placeholders: str = ', '.join(['?' for _ in df_columns])
        df = df.astype('string').fillna('')
        data: Sequence[tuple[Any, ...]] = list(df.itertuples(index=False, name=None))
        if create:
            self.drop_table(table_name)
            self.create_table(table_name, df_columns)
        for start_row in range(0, len(data), max_rows):
            logger.info(f'Inserting rows {start_row + 1} to {min(start_row + max_rows, len(data))}...')
            batch = data[start_row:start_row + max_rows]
            sql = f'INSERT INTO "{table_name}" ({columns_string}) VALUES ({placeholders})'
            self.execute_sql(sql, data=list(batch))

    def execute_sql(self, sql: str, data: list | None = None, return_results: bool = False) -> tuple[list, list]:
        if self.connection is None:
            raise ConnectionError('No active connection to execute SQL.')
        cursor = self.connection.cursor()
        try:
            if data is not None:
                logger.debug(f'Executing SQL: {sql} with {len(data)} data rows.')
                cursor.executemany(sql, data)
            else:
                logger.debug(f'Executing SQL: {sql}')
                cursor.execute(sql)
        except Exception as ex:
            self.connection.rollback()
            logger.error(f'Error executing SQL: {ex}.')
            raise
        else:
            self.connection.commit()
            if return_results:
                results = cursor.fetchall()
                description = list(cursor.description) if cursor.description else []
                return list(results), description
            return [], []

    def drop_table(self, table_name: str) -> None:
        _validate_identifier(table_name)
        logger.debug(f'Dropping table {table_name}...')
        self.execute_sql(f'DROP TABLE IF EXISTS "{table_name}";')

    def create_table(self, table_name: str, columns: list[str]) -> None:
        _validate_identifier(table_name)
        for col in columns:
            _validate_identifier(col)
        logger.debug(f'Creating table {table_name}...')
        cols_sql = ',\n'.join([f'  "{col}" TEXT' for col in columns])
        sql = f'CREATE TABLE "{table_name}" (\n{cols_sql}\n);'
        self.execute_sql(sql)

    def get_columns(self) -> pd.DataFrame:
        """Return a DataFrame with metadata for all columns in the database."""
        if self.connection is None:
            raise ConnectionError('No active connection.')
        cursor = self.connection.cursor()
        res = cursor.execute("SELECT * FROM sqlite_master WHERE type = 'table';")
        results = res.fetchall()
        names = [d[0] for d in cursor.description]
        table_count = len(results)
        logger.info(f'{table_count} tables found')
        columns = []
        for item in results:
            row = {name: item[i] for i, name in enumerate(names)}
            table_name = row['name']
            field_list = row['sql'].split('(')[1].split(')')[0].split(',')
            for field in field_list:
                col: dict[str, str] = {}
                meta = field.strip().split(' ')
                col['table'] = table_name
                col['column_name'] = meta[0]
                if len(meta) > 1:
                    col['datatype'] = meta[1]
                if len(meta) > 2:
                    col['key'] = meta[2]
                columns.append(col)
        logger.info(f'{len(columns)} columns found')
        return pd.DataFrame(columns)

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            logger.debug('SQLite connection closed.')
