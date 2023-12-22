# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
import pandas as pd
import sys
import logging
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(filename)s] [%(funcName)20s()] [%(levelname)s] - %(message)s",
    # stream=sys.stdout,
)
logger = logging.getLogger("STORAGE_DATABASE.CPW_DATA.LOG_OUTPUTS")

def helper_test(session: Session) -> int:
    logger.info("yolo2")
    return 5

def main(session: Session) -> str: 
    # Your code goes here, inside the "main" handler.
    tableName = 'information_schema.packages'
    dataframe = session.table(tableName).filter(col("language") == 'python')

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

    infomation_schema_df.head()

    # Print a sample of the dataframe to standard output.
    dataframe.show()

    test_int = helper_test(session)

    logger.info("Yolo")
    
    # Return value will appear in the Results tab.
    return f"Successfully found {len(infomation_schema_df)} rows. Called test_helper and got {str(test_int)}"