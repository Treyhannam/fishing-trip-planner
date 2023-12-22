""" After raw data is pulled from a webscraper it needs to be parsed into a pandas DataFrame
where it can then be writted into a snowflake table or passed to other processing functions.
"""
import sys
import logging
import pandas as pd
from snowflake.snowpark import Session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(filename)s] [%(funcName)20s()] [%(levelname)s] - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def process_master_angler_data(raw_data: list) -> pd.DataFrame:
    """Given data stored as a list, it will be turned into a pandas
    dataframe.

    :param raw_data: a list of elements that take on the format. Example: ["John\t Catfish\t 23\t Wash Park\t June/2023 \t Yes"]
                    Notably, the results will be an element of n number results seperated by new lines.

    :return: the data as a pandas dataframe. Example:
             ___________________________________________________________
            |Angler |Species    |Length |Location   |Date      |Released|
            |-----------------------------------------------------------|
            |John   |Catfish    |23     |Wash Park  |June/2023 |Yes     |
            |___________________________________________________________|
    """
    all_raw_data = []
    for page_result in raw_data:
        all_raw_data += page_result.split("\n")

    parsed_data = {
        "Angler": [],
        "Species": [],
        "Length": [],
        "Location": [],
        "Date": [],
        "Released": [],
    }
    for row in all_raw_data:
        split_row = row.split("\t")

        if len(split_row) == 6:
            parsed_data["Angler"].append(split_row[0])
            parsed_data["Species"].append(split_row[1])
            parsed_data["Length"].append(split_row[2])
            parsed_data["Location"].append(split_row[3])
            parsed_data["Date"].append(split_row[4])
            parsed_data["Released"].append(split_row[5])

        else:
            if len(split_row[0]) > 0:
                logging.warning(
                    f"Incomplete record with {len(split_row)} item(s). Length of first element: {len(split_row[0])}"
                )

    processed_data = pd.DataFrame(parsed_data)

    logging.info(
        f"Number of elements in raw_data={len(all_raw_data)}. Number of rows in dataframe {len(processed_data)}"
    )

    return processed_data
