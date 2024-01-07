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
logger = logging.getLogger("STORAGE_DATABASE.CPW_DATA.LOG_OUTPUTS")


def combine_trout_tables(session: Session) -> str:
    """Once all the data is in Snowflake all of it will be combined into one table. This process will create
    a new table to store all the data, add a main species column do note the primary species for each row, and insert
    the data from each species table.
    """
    truncate_result = session.sql(
        "truncate STORAGE_DATABASE.CPW_DATA.ALL_SPECIES"
    ).collect()

    logger.info(
        f"Truncated {truncate_result[0][0]} rows from STORAGE_DATABASE.CPW_DATA.ALL_SPECIES"
    )

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

    # Start inserting all the data and populate the Main Species column
    for table_name in infomation_schema_df.TABLE_NAME:
        logger.info(f"Beginning table: {table_name}")

        insert_statement = f"""
        INSERT INTO STORAGE_DATABASE.CPW_DATA.ALL_SPECIES (
            "main_species",
            "fish_species",
            "water",
            "county",
            "property_name",
            "ease_of_access",
            "boating",
            "fishing_pressure",
            "stocked",
            "elevation(ft)",
            "latitude",
            "longitude"
        )

        select
            '{table_name.replace("Trout: ", "")}' as "main_species",
            "Fish Species ",
            "Water",
            "County",
            "Property name",
            "Ease of access",
            "Boating",
            "Fishing pressure",
            "Stocked",
            "Elevation(ft)",
            "Latitude",
            "Longitude"
        FROM STORAGE_DATABASE.CPW_DATA."{table_name}"
        """

        table_insert_result = session.sql(insert_statement).collect()

        logger.info(f"Wrote {table_insert_result[0][0]} rows from table: {table_name}")

    return "Successfully Completed Proceedure. For more info view logs."
