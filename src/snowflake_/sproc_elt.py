"""Snowflake functions to clean or move data.
"""
import sys
import logging
from snowflake.snowpark import Session
import pandas as pd

logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(filename)s] [%(funcName)20s()] [%(levelname)s] - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def combine_trout_tables(session: Session) -> str:
    """Once all the data is in Snowflake all of it will be combined into one table. This process will create
    a new table to store all the data, add a main species column do note the primary species for each row, and insert
    the data from each species table.
    """
    infomation_schema_df = session.sql(
        """
    SELECT
        TABLE_NAME
    FROM storage_database.information_schema.TABLES
    WHERE
        TABLE_NAME like 'Trout%' 
        AND TABLE_SCHEMA = 'CPW_DATA'
    """
    ).to_pandas()

    # Find all the column names we will encounter and their datatype
    ddl_dict = {"Main Species": "VARCHAR(250)"}
    tables_dict = {}
    for table in infomation_schema_df.TABLE_NAME.to_list():
        desc_table_query = session.sql(
            f'DESC TABLE STORAGE_DATABASE.CPW_DATA."{table}"'
        ).collect()

        desc_table_df = pd.DataFrame(desc_table_query)

        tables_dict[table] = desc_table_df

        for row in desc_table_df.itertuples():
            ddl_dict[row.name] = row.type

    # Create the table for all data
    name_and_dtype = [
        f'"{column}" {data_type}' for column, data_type in ddl_dict.items() if column
    ]

    name_and_dtype_with_syntax = ",\n\t".join(name_and_dtype)

    create_statement = f"""
    create or replace table STORAGE_DATABASE.CPW_DATA.ALL_SPECIES (
        {name_and_dtype_with_syntax}
    )
    """

    creation_result = session.sql(create_statement).collect()

    logger.info(f"Result for creating species data: {creation_result[0]}")

    # Start inserting all the data and populate the Main Species column
    for table_name, table_df in tables_dict.items():
        table_df["name_with_quotes"] = '"' + table_df.name + '"'

        table_columns = table_df.loc[
            table_df.name.str.len() > 0
        ].name_with_quotes.to_list()

        table_columns_with_syntax = ",\n\t".join(table_columns)

        insert_statement = f"""
        INSERT INTO STORAGE_DATABASE.CPW_DATA.ALL_SPECIES (
            "Main Species",
            {table_columns_with_syntax}
        )

        select
            '{table_name.replace("Trout: ", "")}' as "Main Species",
            {table_columns_with_syntax}
        FROM STORAGE_DATABASE.CPW_DATA."{table_name}"
        """

        table_insert_result = session.sql(insert_statement).collect()

        logger.info(f"Wrote {table_insert_result[0][0]} rows from table: {table_name}")
        
    return "Done :D "
