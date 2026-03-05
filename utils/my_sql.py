import mysql.connector
from mysql.connector import errorcode, MySQLConnection
import sshtunnel
import pandas as pd
import logging
import re
from typing import Any


def _validate_identifier(name: str) -> str:
    """Validate that a SQL identifier (table, column, schema) contains only safe characters."""
    if not re.match(r'^[\w\s./]+$', name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


logger = logging.getLogger(__name__)

sshtunnel.SSH_TIMEOUT = 15.0
sshtunnel.TUNNEL_TIMEOUT = 15.0


class MySqlConnection:
    """
    A class for connecting to a MySQL database (optionally via SSH tunnel) and executing SQL queries.
    Mirrors the interface of AzureSqlConnection and SqliteConnection for cross-connector compatibility.

    If ssh_host is provided, the connection will be tunnelled through SSH. Otherwise a direct
    TCP connection to host:mysql_port is made.
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str | None = None,
        ssh_host: str | None = None,
        ssh_user: str | None = None,
        ssh_pw: str | None = None,
        ssh_port: int = 22,
        mysql_port: int = 3306,
    ) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_pw = ssh_pw
        self.ssh_port = ssh_port
        self.mysql_port = mysql_port
        self._tunnel: sshtunnel.SSHTunnelForwarder | None = None
        self._connection: MySQLConnection | None = None

    def connect(self) -> MySQLConnection:
        logger.debug('Starting MySQL connection process...')
        if self._connection is not None and self._connection.is_connected():
            return self._connection
        if self.ssh_host:
            self._tunnel = sshtunnel.SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_user,
                ssh_password=self.ssh_pw,
                remote_bind_address=(self.host, self.mysql_port),
            )
            self._tunnel.start()
            logger.info('SSH tunnel established.')
            mysql_host = '127.0.0.1'
            mysql_port = self._tunnel.local_bind_port
        else:
            mysql_host = self.host
            mysql_port = self.mysql_port
        params: dict[str, Any] = {
            'host': mysql_host,
            'port': mysql_port,
            'user': self.user,
            'password': self.password,
        }
        if self.database:
            params['database'] = self.database
        try:
            self._connection = MySQLConnection(**params)
            logger.info('Connected to MySQL database.')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error('Access denied: wrong username or password.')
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error('Database does not exist.')
            else:
                logger.error(err)
            raise
        return self._connection

    def basic_connectivity_test(self) -> None:
        logger.debug('Starting basic connectivity test...')
        if self._connection is None:
            raise ConnectionError('No active connection to test.')
        sql = 'SELECT NOW() as timestamp;'
        results, _ = self.execute_sql(sql, return_results=True)
        for row in results:
            logger.debug(str(row[0]))

    def read_table(self, table_name: str) -> pd.DataFrame:
        _validate_identifier(table_name)
        logger.debug(f'Reading table {table_name}...')
        if self._connection is None:
            raise ConnectionError('No active connection to read data.')
        sql = f'SELECT * FROM {table_name};'
        results, description = self.execute_sql(sql, return_results=True)
        columns = [item[0] for item in description]
        return pd.DataFrame(results, columns=columns)

    def write_table(self, df: pd.DataFrame, table_name: str, create: bool = True) -> None:
        _validate_identifier(table_name)
        logger.debug(f'Writing table {table_name} (create={create})...')
        if self._connection is None:
            raise ConnectionError('No active connection to write data.')
        if create:
            self.drop_table(table_name)
            self.execute_sql("SET sql_mode='ANSI_QUOTES';")
            self.create_table(table_name, df.columns.tolist())
        placeholders = ', '.join(['%s'] * len(df.columns))
        columns_string = ', '.join([f'"{col}"' for col in df.columns])
        sql = f'INSERT INTO {table_name} ({columns_string}) VALUES ({placeholders})'
        data = [tuple(row) for _, row in df.iterrows()]
        self.execute_sql(sql, data=data)

    def execute_sql(self, sql: str, data: list | None = None, return_results: bool = False) -> tuple[list, list]:
        if self._connection is None:
            raise ConnectionError('No active connection to execute SQL.')
        cursor = self._connection.cursor()
        try:
            if data is not None:
                logger.debug(f'Executing SQL: {sql} with {len(data)} data rows.')
                cursor.executemany(sql, data)
            else:
                logger.debug(f'Executing SQL: {sql}')
                cursor.execute(sql)
        except mysql.connector.Error as ex:
            logger.error(f'Error executing SQL: {ex}.')
            raise
        else:
            self._connection.commit()
            if return_results:
                results = cursor.fetchall()
                description = list(cursor.description) if cursor.description else []
                return list(results), description
            return [], []
        finally:
            cursor.close()

    def drop_table(self, table_name: str) -> None:
        _validate_identifier(table_name)
        logger.debug(f'Dropping table {table_name}...')
        self.execute_sql(f'DROP TABLE IF EXISTS {table_name};')

    def create_table(self, table_name: str, columns: list[str]) -> None:
        _validate_identifier(table_name)
        for col in columns:
            _validate_identifier(col)
        logger.debug(f'Creating table {table_name}...')
        cols_sql = ', '.join([f'"{col}" VARCHAR(255)' for col in columns])
        sql = f'CREATE TABLE {table_name} ({cols_sql});'
        self.execute_sql(sql)

    def get_columns(self) -> pd.DataFrame:
        """Return a DataFrame with metadata for all columns in the current database."""
        sql = """
            SELECT TABLE_NAME as `table`, COLUMN_NAME as column_name,
                   DATA_TYPE as datatype, COLUMN_KEY as `key`
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            ORDER BY TABLE_NAME, ORDINAL_POSITION;
        """
        results, description = self.execute_sql(sql, return_results=True)
        columns = [item[0] for item in description]
        return pd.DataFrame(results, columns=columns)

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.debug('MySQL connection closed.')
        if self._tunnel is not None:
            self._tunnel.stop()
            self._tunnel = None
            logger.debug('SSH tunnel closed.')


# ---------------------------------------------------------------------------
# Module-level backward-compatibility shims — deprecated, use MySqlConnection
# ---------------------------------------------------------------------------

def ssh_connect(ssh_host: str, ssh_user: str, ssh_pw: str, host: str) -> sshtunnel.SSHTunnelForwarder:
    """Deprecated: use MySqlConnection instead."""
    tunnel: sshtunnel.SSHTunnelForwarder = sshtunnel.SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_user,
        ssh_password=ssh_pw,
        remote_bind_address=(host, 3306),
        local_bind_address=('127.0.0.1', 3306)
    )
    return tunnel


def mysql_connect(tunnel: sshtunnel.SSHTunnelForwarder, user: str, password: str, database: str | None = None) -> MySQLConnection:
    """Deprecated: use MySqlConnection instead."""
    logger.debug("Connecting to MySQL database...")
    try:
        params = {
            'host': tunnel.local_bind_address[0],
            'user': user,
            'password': password,
        }
        if database:
            params['database'] = database
        conn = MySQLConnection(**params)
        logger.info("Connected to MySQL database!")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("Database does not exist")
        else:
            logger.error(err)
        raise
    return conn


def get_table(ssh_conn: sshtunnel.SSHTunnelForwarder, conn: MySQLConnection, table_name=None) -> pd.DataFrame:
    """Deprecated: use MySqlConnection.read_table() instead."""
    _validate_identifier(table_name)
    logger.debug(f"Fetching table {table_name} from database...")
    with ssh_conn:
        try:
            cursor: mysql.connector.connection.MySQLCursor = conn.cursor()
            sql = f"SELECT * FROM {table_name};"
            logger.debug(f"Executing SQL: {sql}")
            cursor.execute(sql)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=column_names)
        except mysql.connector.Error as err:
            logger.error(f"Error: {err}")
            raise
        finally:
            logger.debug("Closing cursor...")
            cursor.close()
        return df


def write_table(ssh_conn: sshtunnel.SSHTunnelForwarder, conn: MySQLConnection, table_name: str, df: pd.DataFrame):
    """Deprecated: use MySqlConnection.write_table() instead."""
    _validate_identifier(table_name)
    logger.debug(f"Writing DataFrame to table {table_name} in database...")
    with ssh_conn:
        try:
            cursor: mysql.connector.connection.MySQLCursor = conn.cursor()
            sql = f"DROP TABLE IF EXISTS {table_name};"
            cursor.execute(sql)
            sql = "SET sql_mode='ANSI_QUOTES';"
            logger.debug(f"Executing SQL: {sql}")
            cursor.execute(sql)
            sql = f"CREATE TABLE {table_name}"
            columns_with_types = ', '.join([f'"{col}" VARCHAR(255)' for col in df.columns])
            sql += f" ({columns_with_types});"
            logger.debug(f"Executing SQL: {sql}")
            cursor.execute(sql)
            conn.commit()
            for _, row in df.iterrows():
                placeholders = ', '.join(['%s'] * len(row))
                columns = ', '.join(f'"{col}"' for col in row.index)
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, tuple(row))
            conn.commit()
        except mysql.connector.Error as err:
            logger.error(f"Error: {err}")
            raise
