""" Provides functions to write pandas data into snowflake and automates the connection to the database.
Additionally can be used to connect to the db for SQL queries using the build_session function
"""
import os
import sys
import json
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


class SnowflakeDfWriter:
    def __init__(self):
        self.builder_object = self.build_session()

    def open_config(self):
        """Opens the config file that contains our credentials."""
        current_directory = os.getcwd()

        config_file_path = os.path.join(current_directory, "config.json")

        # Open and read the json file
        with open(config_file_path, "r") as config_file:
            config_data = json.load(config_file)

        return config_data

    def build_session(self, add_args: dict = {}) -> Session.SessionBuilder:
        """Builds and returns a session builder object to connect to snowflake

        :param add_args: add additional key work arguements for when we create a session.

        :return: a session builder object for connecting to snowflake
        """
        # Build the connection Parameters
        connection_parameters = self.open_config()

        # If we passed in a additional arguements we will add them
        if add_args:
            connection_parameters.update(add_args)

        # Create our session builder object
        builder_obj = Session.builder.configs(connection_parameters)

        return builder_obj

    def write_table(
        self,
        df: pd.DataFrame,
        fully_qualified_table_name: str,
        overwrite: bool = False,
    ):
        """Given a dataframe and location, this function will write the table to snowflake

        :param df: a pandas dataframe
        :param fully_qualified_table_name: the location to write the table. Must be in format database.schema.table_name
        :param overwrite: If this is True it will truncate and load the data. If False it will append the data.
        """
        database, schema, table = fully_qualified_table_name.split(".")

        with self.builder_object.create() as session:
            result = session.write_pandas(
                df,
                table_name=table,
                database=database,
                schema=schema,
                parallel=4,
                auto_create_table=False,
                overwrite=overwrite,
                table_type="",
            )

            table_name = result.table_name

        logger.info(f" Successfully wrote to {table_name}")

        return None


def combine_trout_tables(session):
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
