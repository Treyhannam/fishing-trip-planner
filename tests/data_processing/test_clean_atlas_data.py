import os
import pytest
import pandas as pd
from unittest.mock import patch
from src.data_processing import clean_atlas_data

def test_split_location_data():

    current_directory = os.getcwd()

    data_directory = os.path.join(current_directory, 'tests\\data')

    test_data_fpath = os.path.join(data_directory, "split_data.txt")

    with open(test_data_fpath, 'r') as file:
        spit_test_data = file.read()

    fishing_information_substring, coordinate_substring = clean_atlas_data.split_location_data(spit_test_data)

    assert fishing_information_substring == "Water: Arthur Lake\nCounty: Chaffee\nProperty name: San Isabel National Forest\nFish species:\nTrout: Cutthroat\nTrout: Golden\nEase of access: Difficult\nBoating: None\nFishing pressure: Low\nStocked: No\nDriving directions\nElevation(ft): 1,000\n"
    assert coordinate_substring == "\nNAD83 UTM Zone 13N: 384447, 4273326Latitude: 38.60092 N    Longitude: -106.32702 W    Decimal Degrees"


def test_parse_location_data():

    current_directory = os.getcwd()

    data_directory = os.path.join(current_directory, 'tests\\data')

    fishing_location_fpath = os.path.join(data_directory, "parse_location.txt")

    coordinates_fpath = os.path.join(data_directory, "parse_coordinates.txt")

    with open(fishing_location_fpath, 'r') as file:
        fishing_location_string = file.read()

    with open(coordinates_fpath, 'r') as file:
        coordinates_string = file.read()

    clean_data = clean_atlas_data.parse_location_data(fishing_location_string, coordinates_string)

    assert clean_data == [   
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
    
def test_align_lists_add_na():
    start_dict = {
        'A' : [1],
        'B' : [2, 5]
    }

    clean_atlas_data.align_lists(start_dict)

    assert start_dict == {
        'A' : [1, "NA"],
        'B' : [2, 5]
    }


def test_align_lists_no_change():

    start_dict = {
        'A' : [1, 1],
        'B' : [2, 5]
    }

    clean_atlas_data.align_lists(start_dict)

    assert start_dict == {
        'A' : [1, 1],
        'B' : [2, 5]
    }


@patch('src.data_processing.clean_atlas_data.split_location_data')
@patch('src.data_processing.clean_atlas_data.parse_location_data')
@patch('src.data_processing.clean_atlas_data.align_lists')
# assert what was passed into clean_atlas()
def test_process_all_location_data(mock_align_lists, mock_parse_location_data, mock_split_location_data):

    mock_split_location_data.return_value = ('A', 'B')
    mock_parse_location_data.return_value = [   
                                                'Fish Species: Cutthroat, Golden',
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

    correct_dict = {
            'Fish Species' : [' Cutthroat, Golden'],
            'Water': [' Arthur Lake'],
            'County': [' Chaffee'],
            'Property name': [' San Isabel National Forest'],
            'Ease of access': [' Difficult'],
            'Boating': [' None'],
            'Fishing pressure': [' Low'],
            'Stocked': [' No'],
            'Elevation(ft)': [' 1,000'],
            'Latitude': [' 38.60092 N'],
            'Longitude':[' -106.32702 W'],
        }

    clean_atlas_data.process_all_location_data(["Starter StringXXXX"])

    assert correct_dict == mock_align_lists.call_args[0][0]
    
