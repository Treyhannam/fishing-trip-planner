""" Sometimes DDL and DML statements are needed to objects within Snowflake. For one time processes a script is in place to execute them.
The overall process will be to execute an sql files in a sequential order. Files will be stored in a pending folder. If the execution is successful
it will be moved to a success folder and an error will go to error folder. Additionally, the error file will cause the script to fail.
"""
import os
import sys
import toml
import shutil
import logging

from snowflake.snowpark import Session

logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(filename)s] [%(funcName)20s()] [%(levelname)s] - %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)

with open("snowflake_setup.toml", "r") as f:
    setup_file = toml.load(f)

# Create the base path for sql folders
working_directory = os.getcwd()
pending_folder = os.path.join(working_directory, "sql_pending")
success_folder = os.path.join(working_directory, "sql_success")

files = os.listdir(pending_folder)

# Create a database connection
builder_obj = Session.builder.configs({
    "account" : os.environ.get('ACCOUNT'),
    "user" :  os.environ.get('USER'),
    "password" :  os.environ.get('PASSWORD'),
}
)

with builder_obj.create() as session:

    # Execute one time queries
    for file in files:

        # read
        fpath = os.path.join(pending_folder, file)

        with open(fpath, mode='r') as sql_script:
            result = session.sql(sql_script).collect()

        logger.info(
            f"Query result: {result[0]}"
        )

        # Move to the success folder
        destination_path = os.path.join(success_folder, file)
        shutil.move(fpath, destination_path)

        logger.info(
            f"Moved {file} to success folder"
        )