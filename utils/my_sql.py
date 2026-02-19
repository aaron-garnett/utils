import mysql.connector
from mysql.connector import errorcode, MySQLConnection
import sshtunnel
import pandas as pd
import logging

logger = logging.getLogger(__name__)

sshtunnel.SSH_TIMEOUT = 10.0
sshtunnel.TUNNEL_TIMEOUT = 10.0


def ssh_connect(ssh_host: str, ssh_user: str, ssh_pw: str, host: str) -> sshtunnel.SSHTunnelForwarder:
    """Establishes an SSH tunnel to the MySQL server."""
    logger.debug("Establishing SSH tunnel...")
    sshtunnel.SSH_TIMEOUT = 15.0
    sshtunnel.TUNNEL_TIMEOUT = 15.0
    tunnel: sshtunnel.SSHTunnelForwarder = sshtunnel.SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_user,
        ssh_password=ssh_pw,
        remote_bind_address=(host, 3306),
        local_bind_address=('127.0.0.1', 3306)
    )
    return tunnel


def mysql_connect(tunnel: sshtunnel.SSHTunnelForwarder, user: str, password: str, database: str | None = None) -> MySQLConnection:
    """Connects to the MySQL database through the SSH tunnel."""
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

    return conn


def get_table(ssh_conn: sshtunnel.SSHTunnelForwarder, conn: MySQLConnection, table_name=None) -> pd.DataFrame:
    """Fetches a table from the MySQL database and returns it as a DataFrame."""
    logger.debug(f"Fetching table {table_name} from database...")
    with ssh_conn:
        try:
            cursor: mysql.connector.connect.cursor = conn.cursor()
            sql = f"SELECT * FROM {table_name};"
            logger.debug(f"Executing SQL: {sql}")
            cursor.execute(sql)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description] # Extract column names
            df = pd.DataFrame(rows, columns=column_names)
        except mysql.connector.Error as err:
            logger.error(f"Error: {err}")
        finally:
            logger.debug("Closing cursor...")
            cursor.close()
        return df


def write_table(ssh_conn: sshtunnel.SSHTunnelForwarder, conn: MySQLConnection, table_name: str, df: pd.DataFrame):
    """Writes a DataFrame to a table in the MySQL database."""
    logger.debug(f"Writing DataFrame to table {table_name} in database...")
    with ssh_conn:
        try:
            cursor: mysql.connector.connect.cursor = conn.cursor()
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
