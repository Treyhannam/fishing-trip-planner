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
        )
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

    second_call = """
        INSERT INTO STORAGE_DATABASE.CPW_DATA.ALL_SPECIES (
            "Main Species",
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
        )

        select
            'Brook' as "Main Species",          
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
        FROM STORAGE_DATABASE.CPW_DATA."Trout: Brook"
        """

    def remove_newline_and_tab_chars(x: str):
        return ' '.join(x.split())

    assert remove_newline_and_tab_chars(mock_session.sql.call_args_list[2][0][0]) == remove_newline_and_tab_chars(second_call)
