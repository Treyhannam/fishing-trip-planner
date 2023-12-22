""" The Colorado Fishing Atlas contains data points for bodies of water to fish. There are two core pieces of data 
data that are desired.

The Fishing Information Point Data

        Water: Arthur Lake
        County: Chaffee
        Property name: San Isabel National Forest
        Fish species:
        Trout: Cutthroat
        Trout: Golden
        Ease of access: Difficult
        Boating: None
        Fishing pressure: Low
        Stocked: No
        Driving directions
        Elevation(ft): 1,000

and the coordinates:

    NAD83 UTM Zone 13N: 384447, 4273326Latitude: 38.60092 N    Longitude: -106.32702 W    Decimal Degrees

However there can be other elements within the text field so a level of parsing with regular expressions that needs to be
done. Once it is done the information above is turned into a dictionary and then final a dataframe

Data cleaned up into a dictionary:

    {
        'Fish Species' : ['Cutthroat, Golden'],
        'Water': ['Arthur Lake'],
        'County': ['Chaffee'],
        'Property name': ['San Isabel National Forest'],
        'Ease of access': ['Difficult'],
        'Boating': ['None'],
        'Fishing pressure': ['Low'],
        'Stocked': ['No'],
        'Elevation(ft)': ['1,000'],
        'Latitude': ['38.60092 N'],
        'Longitude':[ '-106.32702 W'],
    }

Data as a table...

     ______________________________________________________________________________________________________________________________________________________________
    |Fish Species        |Water        |County  |Property name               |Ease of access  |Fishing pressure |Stocked |Elevation(ft) |Latitude   | Longitude    |
    |--------------------------------------------------------------------------------------------------------------------------------------------------------------|
    |Cutthroat, Golden   |Arthur Lake  |Chaffee |San Isabel National Forest  |Difficult       |Low              |No      |1,000         |38.60092 N | -106.32702 W |
    |______________________________________________________________________________________________________________________________________________________________|

Sometimes there may be additional columns that are not always populated. For Pandas dataframes every column needs to have the same number of data. So nulls will be 
added to maintain that requirement.
"""
import sys
import logging
import re
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(filename)s] [%(funcName)20s()] [%(levelname)s] - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def split_location_data(full_location_string: str) -> (str, str):
    """Fishing data for each location has multiple sections of data. Only interested in two chunks of text.
    To parse a regular expression will use the name of the location and the last datapoint to pick it out.
    This will define the beginning and ending endpoint of the first substring. The second substring can be parsed
    by perfoming a split on "Zoom to" and returning the second result.

    :param full_location_string: fishing data pulled from the Colorado Fishing Atlas for a body of water. Additionally,
    the location name appended to the front with XXXX (Lake Name XXXX) and the locations coordinates appended to the end.

    :param return: two substrings of data. The first is data specific to the location and second contains
    the locations coordinates
    """
    body_of_water = full_location_string.split("XXXX")[0]

    split_location_data = [
        text_data
        for text_data in full_location_string.split("Fishing Information Point")
        if body_of_water in text_data and "Fish species:\n" in text_data
    ]

    if len(split_location_data) == 1:
        logger.info(f"Found data for {body_of_water}")

        body_of_water_data_string = split_location_data[0]
    else:
        logger.warning(
            f"Did not find excactly 1 match for location: {body_of_water}. Found {len(split_location_data)} matches"
        )

        return None, None

    start_index = body_of_water_data_string.find(f"Water: {body_of_water}")

    end_index = (
        body_of_water_data_string[start_index::].find("Elevation(ft): ")
        + len(f"Elevation(ft): 00,000")
        + start_index
    )

    return (
        body_of_water_data_string[start_index:end_index],
        full_location_string.split("Zoom to")[1],
    )


def parse_location_data(information_substring: str, coordinates: str) -> list:
    """With location data and the coordinate substrings, the desired data will be parsed into a list.
    Each element in a list will represent a datapoint about the location and text values will resember a dictionary
    (Elevation(ft): 12,000).

    :param location_data: substring of datapoints for a given body of water. Example...

        Water: Arthur Lake
        County: Chaffee
        Property name: San Isabel National Forest
        Fish species:
        Trout: Cutthroat
        Trout: Golden
        Ease of access: Difficult
        Boating: None
        Fishing pressure: Low
        Stocked: No
        Driving directionsgggggggggggggggy
        NAD83 UTM Zone 13N: 384447, 4273326Latitude: 38.60092 N    Longitude: -106.32702 W    Decimal Degrees

    :return: the data as a list. Example..

        [
            'Fish Species : Cutthroat, Golden',
            'Water: Arthur Lake',
            'County: Chaffee',
            'Property name: San Isabel National Forest',
            'Ease of access: Difficult',
            'Boating: None',
            'Fishing pressure: Low',
            'Stocked: No',
            'Elevation(ft): 1,000',
            'Latitude: 38.60092 N',
            'Longitude: -106.32702 W'
        ]
    """
    # Process species data seperately
    raw_species_data = re.findall(
        "Fish species:.*Ease", information_substring, re.DOTALL
    )[0]

    raw_species_data = raw_species_data.replace("\nEase", "")

    raw_species_data = raw_species_data.replace("\n", ",")

    raw_species_data = re.sub("[a-zA-Z]+:", "", raw_species_data)

    clean_species_data = raw_species_data.replace("Fish ,", "Fish Species :")

    clean_fishing_data = [clean_species_data]

    # Process rest of location data
    raw_fishing_information_no_species = re.sub(
        "Fish species:.*Ease", "Ease", information_substring, flags=re.DOTALL
    )

    raw_fishing_information_no_species = raw_fishing_information_no_species.replace(
        "Driving directions", ""
    )

    for raw_data_point in raw_fishing_information_no_species.split("\n"):
        if raw_data_point and ":" in raw_data_point:
            clean_fishing_data.append(raw_data_point)

        elif raw_data_point and ":" not in raw_data_point:
            logger.debug(f"Element {raw_data_point} does not contain a :")

    # Process Coordinates
    latitude_pattern = re.compile(r"Latitude: (\d+\.\d+) ([NS])")

    lat_match = latitude_pattern.search(coordinates).group(0)

    clean_fishing_data.append(lat_match)

    longitude_pattern = re.compile(r"Longitude: (-?\d+\.\d+) ([EW])")

    long_match = longitude_pattern.search(coordinates).group(0)

    clean_fishing_data.append(long_match)

    return clean_fishing_data


def align_lists(clean_data: dict) -> None:
    """Given a dictionary with lists as the values, this function will add nulls to keep the lengths
    of all the lists the same. Specifically as data is processed there may be instances where data is missing for
    a certain key due to processing error or it wasn't available to begin with.

    This will modify the dictionary in place.

    :param clean_data: Dictionary of processed data
    """
    # If a key was missing add a null for it
    column_lengths = {"starter_key": 0}

    largest_key = "starter_key"

    for k, v in clean_data.items():
        if column_lengths[largest_key] < len(v):
            largest_key = k

        column_lengths[k] = len(v)

    for k in clean_data.keys():
        if column_lengths[k] < column_lengths[largest_key]:
            nulls_to_add = column_lengths[largest_key] - column_lengths[k]

            logger.debug(
                f"The key {k} did not have an entry adding {nulls_to_add} NA value(s)"
            )

            clean_data[k] += ["NA"] * nulls_to_add

    return clean_data


def process_all_location_data(raw_data: list) -> pd.DataFrame:
    """This function will parse a list of strings into a pandas dataframe. Each element will need
    to be parsed then added to a final dictionary. Once the final dictionary is complete it will be turned into
    a pandas dataframe.

    :raw_data: list of strings where each element represents text scrapped from a website for a specific location.
    """
    clean_data = {}
    for all_location_data in raw_data:
        if "Loading..." in all_location_data:
            logger.warning(
                "Data did not load for: {}".format(all_location_data.split("XXXX")[1])
            )
            continue

        raw_location_fishing_information, raw_coordinates = split_location_data(
            all_location_data
        )

        if not raw_location_fishing_information:
            continue

        clean_fishing_data = parse_location_data(
            raw_location_fishing_information, raw_coordinates
        )

        # Store the data into a dictionary
        longest_value = 0

        for data_point in clean_fishing_data:
            k, v = data_point.split(":")

            try:
                clean_data[k].append(v)

                longest_value = len(clean_data[k])

            except KeyError:
                # Adding a column while other rows have been populated require NA values
                # to be backfilled to keep all arrays in the dictionary the same length.
                # Approach assumes that new columns will never be the first element in clean_fishing_information
                if longest_value > 1:
                    clean_data[k] = ["NA"] * (longest_value - 1) + [v]

                    logger.debug(
                        f"Key {k} initialed after data has been entered. Backfilling {longest_value-1} rows."
                    )
                else:
                    clean_data[k] = [v]

                    logger.debug(
                        f"Experienced Key error for first entry. Resolving by creating a new Key for {k}"
                    )

        clean_data = align_lists(clean_data)

    return pd.DataFrame(clean_data)
