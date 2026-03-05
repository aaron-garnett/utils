"""
Google Cloud BigQuery connectivity.

Authentication options:
  1. Service account JSON key file — pass credentials_path='/path/to/key.json'
  2. Application Default Credentials (ADC) — omit credentials_path; relies on
     `gcloud auth application-default login` or a GCP-managed identity.

Interface note: BigQuery uses a client API, not DB-API 2.0 cursors.
  - execute_sql() returns a pd.DataFrame (not a tuple of rows/description).
  - write_table() uses the BigQuery Storage Write API via load_table_from_dataframe().
  - DDL statements (create_table, drop_table) are executed via execute_sql().

Dependencies (add to pyproject.toml under [project.optional-dependencies] bigquery):
  google-cloud-bigquery>=3.0
  google-cloud-bigquery-storage>=2.0   (optional: speeds up read_table)
  pyarrow>=10.0                         (required by bigquery pandas integration)
"""

import pandas as pd
import logging
import re

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
except ImportError as _e:
    raise ImportError(
        "google-cloud-bigquery is required for BigQueryConnection. "
        "Install it with: pip install 'utils[bigquery]'"
    ) from _e


def _validate_identifier(name: str) -> str:
    """Validate that a BigQuery identifier contains only safe characters."""
    if not re.match(r'^[\w\s.`-]+$', name):
        raise ValueError(f"Invalid BigQuery identifier: {name!r}")
    return name


logger = logging.getLogger(__name__)


class BigQueryConnection:
    """
    A class for connecting to Google Cloud BigQuery and executing SQL queries.

    The interface mirrors AzureSqlConnection, MySqlConnection, SqliteConnection, and
    SnowflakeConnection, with the following BigQuery-specific adaptations:

      - execute_sql() returns a pd.DataFrame rather than (rows, description) because
        BigQuery's API is query-job based, not cursor-based.
      - write_table() uses bigquery.LoadJobConfig with WRITE_TRUNCATE when create=True.
      - Tables are referenced as dataset.table (schema = dataset_id here).
    """

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        credentials_path: str | None = None,
    ) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.credentials_path = credentials_path
        self._client: bigquery.Client | None = None

    def connect(self) -> bigquery.Client:
        logger.debug(f'Connecting to BigQuery project={self.project_id} dataset={self.dataset_id}...')
        if self._client is not None:
            return self._client
        if self.credentials_path:
            logger.debug('Using service account credentials.')
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            self._client = bigquery.Client(project=self.project_id, credentials=credentials)
        else:
            logger.debug('Using Application Default Credentials.')
            self._client = bigquery.Client(project=self.project_id)
        logger.info(f'Connected to BigQuery: {self.project_id}.{self.dataset_id}')
        return self._client

    def basic_connectivity_test(self) -> None:
        logger.debug('Starting basic connectivity test...')
        if self._client is None:
            raise ConnectionError('No active connection to test.')
        result = self.execute_sql('SELECT current_timestamp() as timestamp')
        logger.debug(str(result['timestamp'].iloc[0]))

    def read_table(self, table_name: str) -> pd.DataFrame:
        _validate_identifier(table_name)
        logger.debug(f'Reading table {table_name}...')
        if self._client is None:
            raise ConnectionError('No active connection to read data.')
        full_name = self._full_table_name(table_name)
        return self.execute_sql(f'SELECT * FROM `{full_name}`')

    def write_table(self, df: pd.DataFrame, table_name: str, create: bool = True) -> None:
        _validate_identifier(table_name)
        logger.debug(f'Writing table {table_name} (create={create})...')
        if self._client is None:
            raise ConnectionError('No active connection to write data.')
        full_name = self._full_table_name(table_name)
        table_ref = bigquery.TableReference.from_string(full_name, default_project=self.project_id)
        write_disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE if create
            else bigquery.WriteDisposition.WRITE_APPEND
        )
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True,
        )
        job = self._client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # wait for completion
        logger.info(f'Wrote {len(df)} rows to {full_name}.')

    def execute_sql(self, sql: str) -> pd.DataFrame:
        """Execute a SQL statement and return results as a DataFrame.

        For DDL statements (CREATE, DROP, INSERT) the returned DataFrame will be empty.
        BigQuery does not support parameterised DML via executemany; use write_table() for bulk loads.
        """
        if self._client is None:
            raise ConnectionError('No active connection to execute SQL.')
        logger.debug(f'Executing SQL: {sql}')
        query_job = self._client.query(sql)
        result = query_job.result()
        return result.to_dataframe()

    def drop_table(self, table_name: str) -> None:
        _validate_identifier(table_name)
        logger.debug(f'Dropping table {table_name}...')
        full_name = self._full_table_name(table_name)
        self.execute_sql(f'DROP TABLE IF EXISTS `{full_name}`;')

    def create_table(self, table_name: str, columns: list[str]) -> None:
        _validate_identifier(table_name)
        for col in columns:
            _validate_identifier(col)
        logger.debug(f'Creating table {table_name}...')
        full_name = self._full_table_name(table_name)
        cols_sql = ',\n  '.join([f'`{col}` STRING' for col in columns])
        sql = f'CREATE TABLE `{full_name}` (\n  {cols_sql}\n);'
        self.execute_sql(sql)

    def get_columns(self) -> pd.DataFrame:
        """Return a DataFrame with metadata for all columns in the current dataset."""
        sql = f"""
            SELECT table_name as `table`, column_name, data_type as datatype, NULL as `key`
            FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            ORDER BY table_name, ordinal_position;
        """
        return self.execute_sql(sql)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            logger.debug('BigQuery client closed.')

    def _full_table_name(self, table_name: str) -> str:
        """Return a fully-qualified BigQuery table name: project.dataset.table."""
        if '.' in table_name:
            return table_name
        return f'{self.project_id}.{self.dataset_id}.{table_name}'

    def __repr__(self) -> str:
        return f'BigQueryConnection({self.project_id}.{self.dataset_id})'
