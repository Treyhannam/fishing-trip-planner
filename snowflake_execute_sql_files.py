""" Sometimes DDL and DML statements are needed to objects within Snowflake. For one time processes a script is in place to execute them.
The overall process will be to execute an sql files in a sequential order. Files will be stored in a pending folder. If the execution is successful
it will be moved to a success folder and an error will go to error folder. Additionally, the error file will cause the script to fail.
"""
import os
import sys
import glob
import logging
from snowflake.snowpark import Session

logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(filename)s] [%(funcName)20s()] [%(levelname)s] - %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)

# Find the SQL files to execute
working_directory = os.getenv('GITHUB_WORKSPACE')

sql_folder_search_pattern = os.path.join(working_directory, "*_sql_files")

sql_folder_search_result = glob.glob(sql_folder_search_pattern)

if len(sql_folder_search_result) != 1:
    raise ValueError(f"Expected excactly 1 match when searching for folders that end with '_sql_files'. Found {len(sql_folder_search_result)} matches")

sql_folder = sql_folder_search_result[0]

sql_folder_name = os.path.basename(sql_folder)

folder_order = [int(folder_name.split("_", 1)[0]) for folder_name in os.listdir(sql_folder)]

max_sprint_folder_prefix = str(max(folder_order))+"_*"

max_sprint_folder_search_pattern = os.path.join(sql_folder, max_sprint_folder_prefix)

max_sprint_folder_path = glob.glob(max_sprint_folder_search_pattern)[0]

max_sprint_folder_name = os.path.basename(max_sprint_folder_path)

sprint_sql_files = os.listdir(max_sprint_folder_path)

# Create a database connection
builder_obj = Session.builder.configs({
    "account" : os.environ.get('ACCOUNT'),
    "user" :  os.environ.get('USER'),
    "password" :  os.environ.get('PASSWORD'),
}
)

# Connect to the database and deploy files that have not been executed
sprint_query = f"""
SELECT
    file_name
FROM STORAGE_DATABASE.CPW_DATA.SQL_DEPLOYMENT_HISTORY
WHERE
    project_name = '{sql_folder_name}'
    and sprint_folder_name = '{max_sprint_folder_name}'
"""

with builder_obj.create() as session:

    executed_files_df = session.sql(sprint_query).to_pandas()

    executed_file_names = executed_files_df.FILE_NAME.to_list()

    # Execute one time queries
    for file in sprint_sql_files:

        if file not in executed_file_names:
            logger.info(
                f"Starting file: {file}"
            )
            # read and run files
            fpath = os.path.join(max_sprint_folder_path, file)

            logger.info(
                f"Using file path: {fpath}"
                )

            with open(fpath, mode='r') as sql_script:
                query = sql_script.read()

                logger.info(
                    f"Running Query \n {query}"
                )

                result = session.sql(query).collect()

            result_as_string = str(result[0]).replace("Row(", "").replace(")", "")

            insert_query = f"""
            INSERT INTO STORAGE_DATABASE.CPW_DATA.SQL_DEPLOYMENT_HISTORY (project_name, sprint_folder_name, file_name, execution_result)
            values ('{sql_folder_name}', '{max_sprint_folder_name}', '{file}', '{result_as_string[0:10_000]}')
            """

            logger.info(
                f"Insert query \n {insert_query}"
            )

            _ = session.sql(insert_query).collect()
        else:
            logger.info(f"Already Executed {file}")
