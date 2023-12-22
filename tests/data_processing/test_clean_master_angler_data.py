import os
import pandas as pd
from src.data_processing import clean_master_angler_data
from unittest import mock

def test_process_master_angler_data():
    
    current_directory = os.getcwd()

    data_directory = os.path.join(current_directory, 'tests\\data')

    master_angler_data_fpath = os.path.join(data_directory, "master_angler_data.txt")

    # This file represents one element in a list
    master_angler_data = []
    with open(master_angler_data_fpath, 'r') as file:
        master_angler_data.append(file.read())
    
    processed_df = clean_master_angler_data.process_master_angler_data(master_angler_data)

    correct_df = pd.DataFrame(
        {
            "Angler" : ["Trey", "Tanner"],
            "Species" : ["Catfish", "Catfish"],
            "Length" : ["23", "38"],
            "Location" : ["Lon Hagler", "Boyd Lake"],
            "Date" : ["July\\2023", "June\\2023"],
            "Released" : ["Yes", "No"],
        }
    )

    column_order = correct_df.columns 

    assert correct_df[column_order].equals(processed_df[column_order])
    