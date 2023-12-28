import os
import pytest
import pandas as pd
from unittest.mock import Mock
from src.snowflake_ import combine_trout_data

def test_combine_trout_tables():

    mock_session = Mock()

    mock_dict = {"""
    SELECT
        TABLE_NAME
    FROM storage_database.information_schema.TABLES
    WHERE
        TABLE_NAME like 'Trout%' 
        AND TABLE_SCHEMA = 'CPW_DATA'
    """ : pd.DataFrame(
            {
                "TABLE_NAME" : ["Trout: Brook"]
            }
        ),
    'DESC TABLE STORAGE_DATABASE.CPW_DATA."Trout: Brook"': {
            "name" : ["Fish Species", "Water"],
            "type" : ["VARCHAR(16777216)", "VARCHAR(16777216)"]
        }
    }

    class SQLResult:
        def __init__(self, value):
            self.value = value

        def collect(self):
            return self.value
        
        def to_pandas(self):
            return self.value

    mock_session.sql.return_value = "test" 

    def mock_sql(query):
        result_value = mock_dict.get(query, "test")
        return SQLResult(result_value)

    mock_session.sql.side_effect = mock_sql

    combine_trout_data.combine_trout_tables(mock_session)

    third_call = """
    create or replace table STORAGE_DATABASE.CPW_DATA.ALL_SPECIES (
        DIM_ALL_SPECIES_ID NUMBER(38, 0) autoincrement start 0 increment by 1,
        "Main Species" VARCHAR(250),
        "Fish Species" VARCHAR(16777216),
        "Water" VARCHAR(16777216)
    )
    """

    fourth_call = """
        INSERT INTO STORAGE_DATABASE.CPW_DATA.ALL_SPECIES (
            "Main Species",
            "Fish Species",
            "Water"
        )

        select
            'Brook' as "Main Species",
            "Fish Species",
            "Water"
        FROM STORAGE_DATABASE.CPW_DATA."Trout: Brook"
    """

    def remove_newline_and_tab_chars(x: str):
        return ' '.join(x.split())

    assert remove_newline_and_tab_chars(mock_session.sql.call_args_list[2][0][0]) == remove_newline_and_tab_chars(third_call)
    assert remove_newline_and_tab_chars(mock_session.sql.call_args_list[3][0][0]) == remove_newline_and_tab_chars(fourth_call)
