import pandas as pd
import numpy as np
import sqlite3


def compare_dataframes(df1, df2):
    """Compare two DataFrames and return the number of rows that are the same and different."""
    # Ensure the DataFrames have the same columns
    if not df1.columns.equals(df2.columns):
        raise ValueError("DataFrames must have the same columns")

    # Merge the two DataFrames on all columns to find matching rows
    merged = df1.merge(df2, on=list(df1.columns), how='outer', indicator=True)

    # Count the number of rows that are the same or different
    same_rows = merged[merged['_merge'] == 'both'].shape[0]
    different_rows = merged[merged['_merge'] != 'both'].shape[0]

    exceptions = merged[merged['_merge'] != 'both']

    print(exceptions.head(50).to_markdown())

    return same_rows, different_rows


def compare_tables(dataframe1, dataframe2):
    """Compare two tables and return a dictionary with the number of rows and columns that are the same and different."""
    # convert all fields to string
    df1 = dataframe1.astype('string').replace(np.nan, '').map(lambda x: x.strip())
    df2 = dataframe2.astype('string').replace(np.nan, '').map(lambda x: x.strip())

    # identify common columns
    df1_columns = df1.columns.tolist()
    df2_columns = df2.columns.tolist()
    common_columns = list(set(df1_columns) & set(df2_columns))
    print(f'Table 1 has {len(df1_columns)} total columns.')
    print(f'Table 2 has {len(df2_columns)} total columns.')
    print(f'Two tables have {len(common_columns)} columns in common\n')

    # identify common records based on shared index
    df1_records = df1.index.tolist()
    df2_records = df2.index.tolist()
    common_records = list(set(df1_records) & set(df2_records))
    print(f'Table 1 has {len(df1)} rows.')
    print(f'Table 2 has {len(df2)} rows.')
    print(f'Based on their index, the two tables have {len(common_records)} records in common\n')

    # for each common column, identify number of matching values
    for column in common_columns:
        s1 = df1[column]
        s2 = df2[column]
        df1_values = set(s1)
        df2_values = set(s2)
        shared_values = set(df1_values & df2_values)
        df2_unique = list(df2_values - shared_values)
        print(f'"{column}" (values): table 1 ({len(df1_values)}); table 2 ({len(df2_values)}); combined ({len(shared_values)})')
        print(df2_unique)

    df = (df1.isin(df2)
          .transpose()
          .loc[common_columns]
          .stack()
          .groupby(level=0)
          .value_counts()
          .unstack(fill_value=0)
          )
    df['match_rate'] = df[True] / (df[False] + df[True])
    columns_matching = len(df.loc[df['match_rate'] == 1])
    # return results
    result_dict = {
        'rows_table1': len(dataframe1),
        'rows_table2': len(dataframe2),
        'rows_both': len(common_records),
        'cols_table1': len(df1_columns),
        'cols_table2': len(df2_columns),
        'cols_both': len(common_columns),
        'cols_matching': columns_matching,
        'df': df
    }
    return result_dict


def find_value(connection, schema_columns, datatype, value):
    """Find a specific value in the database and return the tables and columns where it is found."""
    if datatype is not None:
        df = schema_columns[schema_columns['datatype'] == datatype]
    else:
        df = schema_columns
    for index, row in df.iterrows():
        table = row['table']
        column = row['column_name']
        result = get_column_values(connection, table, column)
        if value in result:
            print(f'Found match in {table}.{column}!')
    return


def find_primary_key(connection, foreign_key_table, foreign_key_column, schema_columns, keys_only=False, strict_type=True):
    """Find potential primary keys for a given foreign key column."""
    df = schema_columns
    foreign_key = df[(df['table'] == foreign_key_table) & (df['column_name'] == foreign_key_column)]
    if len(foreign_key) == 0:
        print(f'{foreign_key_table}.{foreign_key_column} not found in database.')
        return
    df = df[~((df['table'] == foreign_key_table) & (df['column_name'] == foreign_key_column))]
    if strict_type:
        datatype = foreign_key['datatype'].item()
        df = df[(df['datatype'] == datatype)]
    if keys_only:
        df = df[df['key'].notna()]
    print(f'Scanning {len(df)} potential columns')
    values = list(set(get_column_values(connection, foreign_key_table, foreign_key_column)))
    if not strict_type:
        values = [str(value) for value in values]
    print(f'There are {len(list(set(values)))} unique values in {foreign_key_table}.{foreign_key_column}')
    results = []
    for index, row in df.iterrows():
        table = row['table']
        column = row['column_name']
        key_values = set(get_column_values(connection, table, column))
        if not strict_type:
            key_values = set([str(value) for value in list(key_values)])
        key_values_matching = list(key_values.intersection(set(values)))
        unused_values = list(set(key_values) - set(values))
        percent_match = len(key_values_matching) / len(values)
        results.append({
            'table': table,
            'column': column,
            'matched_values': len(key_values_matching),
            'unused_values': len(unused_values),
            'percent_match': percent_match,
        })
    df = pd.DataFrame(results)
    return df


def get_column_values(connection, table, column, value=None):
    """Get all values from a specific column in a table, optionally filtering by a specific value."""
    cur = connection.cursor()
    sql = f'select "{column}" from {table}'
    if value:
        sql += f" where {column} = {value};"
    else:
        sql += ';'
    res = cur.execute(sql)
    results = res.fetchall()
    values = [item[0] for item in results]
    return values


def get_columns_sqlite(connection):
    """Get all columns in the SQLite database along with their metadata."""
    cur = connection.cursor()
    sql = """
        select * from sqlite_master
        where type = 'table';
    """
    res = cur.execute(sql)
    results = res.fetchall()

    table_count = len(results)
    names = [description[0] for description in cur.description]

    print(f'{table_count} tables found')
    columns = []
    for item in results:
        row = {name: item[i] for i, name in enumerate(names)}
        table_name = row['name']
        field_list = row['sql'].split('(')[1].split(')')[0].split(',')
        for item in field_list:
            column = {}
            column_metadata = item.strip().split(' ')
            column['table'] = table_name
            column['column_name'] = column_metadata[0]
            if len(column_metadata) > 1:
                column['datatype'] = column_metadata[1]
            if len(column_metadata) > 2:
                column['key'] = column_metadata[2]
            columns.append(column)
    print(f'{len(columns)} columns found')
    df = pd.DataFrame(columns)
    return df


def connect_to_sqlite(file_path):
    """Connect to a SQLite database and return the connection object."""
    connection = sqlite3.connect(file_path)
    return connection
