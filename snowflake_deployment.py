""" This script connects to snowflake and creates a stage to store python code for stored procedures
and then creates the proceedures. 
"""
import os
import sys
import toml
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

# Create the base path for procedures
working_directory = os.getcwd()
snowflake_folder = os.path.join(working_directory, "src\\snowflake_")

builder_obj = Session.builder.configs({
    "account" : os.environ.get('ACCOUNT'),
    "user" :  os.environ.get('USER'),
    "password" :  os.environ.get('PASSWORD'),
}
)

with builder_obj.create() as session:
    # Create stage if it does not exist
    create_statement_result = session.sql(
        "CREATE STAGE IF NOT EXISTS {}".format(
            setup_file["stage"]["fully_qualified_name"]
        )
    ).collect()

    logger.info(
        f"Successfully created table. Statement result: {create_statement_result[0]}"
    )

    # Deploy Procedures
    for procedure in setup_file["procedures"]:
        session.use_database(setup_file["stage"]["stage_database"])
        session.use_schema(setup_file["stage"]["stage_schema"])

        new_procedure = session.sproc.register_from_file(
            file_path=os.path.join(snowflake_folder, procedure["fname"]),
            name=procedure["procedure_name"],
            func_name=procedure["function_name"],
            stage_location=setup_file["stage"]["fully_qualified_name"],
            packages=procedure["packages"],
            is_permanent=True,
            replace=True,
            source_code_display=True,
            execute_as="owner",
        )

        logger.info(
            "Created procedure: {} For file: {}".format(new_procedure.name, procedure["fname"])
        )
        # This WILL fail since i have to specify arguements, make this an arguement for setup file
        set_log_level_result = session.sql(f"ALTER PROCEDURE {new_procedure.name}() SET LOG_LEVEL = INFO").collect()

        logger.info(
            set_log_level_result[0][0]
        )