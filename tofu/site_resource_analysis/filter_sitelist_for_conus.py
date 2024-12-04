import pandas as pd
from tofu import OUTPUT_DIR,DATA_DIR,INPUT_DIR
import os
import numpy as np
from tofu.utilities.file_utilities import load_yaml, check_create_folder

def get_conus_sitelist(data_folder = None,sitelist_data_filename = "LC_facility_parcels_NREL_9_27.csv"):
    non_conus = ['HI','VI','MP','GU','AK','AS','PR']
    
    sitelist_data_filename = "LC_facility_parcels_NREL_9_27.csv"
    if data_folder is None:
        sitelist_filepath = os.path.join(str(DATA_DIR),sitelist_data_filename)
    else:
        sitelist_filepath = os.path.join(data_folder,sitelist_data_filename)
    columns = ["parcel_lid","MatchID","state","latitude","longitude","parcel_latitude","parcel_longitude","wind_ground_area","under_1_acre"]
    df  = pd.read_csv(sitelist_filepath,usecols=columns,encoding = "ISO-8859-1")

    for n in non_conus:
        df = df[df["state"] != n]
    return df
# []

# cdf = get_conus_sitelist()
# []