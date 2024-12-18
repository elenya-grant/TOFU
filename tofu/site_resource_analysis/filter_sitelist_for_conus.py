import pandas as pd
from tofu import OUTPUT_DIR,DATA_DIR,INPUT_DIR
import os
import numpy as np
from tofu.utilities.file_utilities import load_yaml, check_create_folder

def get_conus_sitelist(data_folder = None,sitelist_data_filename = "LC_facility_parcels_NREL_9_27.csv"):
    non_conus = ['HI','VI','MP','GU','AK','AS','PR']
    
    if data_folder is None:
        sitelist_filepath = os.path.join(str(DATA_DIR),sitelist_data_filename)
    else:
        sitelist_filepath = os.path.join(data_folder,sitelist_data_filename)
    columns = ["parcel_lid","MatchID","state","latitude","longitude","parcel_latitude","parcel_longitude","wind_ground_area","under_1_acre","wind_exclusion"]
    df  = pd.read_csv(sitelist_filepath,usecols=columns,encoding = "ISO-8859-1")

    for n in non_conus:
        df = df[df["state"] != n]
    df = df.dropna(axis=0,how='any',subset=["latitude","longitude","MatchID"])
    # lat_min, lon_min, lat_max, lon_max
    return df
# []
def filter_sitelist_for_bounds(sitelist):
    # wtk_bounds = (23.83350372314453, -129.22923278808594), (49.35559844970703, -65.714599609375)
    # nsrdb_bounds = (-20.989999771118164, -179.97999572753906), (59.970001220703125, -22.5)
    
    wtk_lat_bounds = [23.83350372314453,49.35559844970703]
    wtk_lon_bounds = [-129.22923278808594,-65.714599609375]

    nsrdb_lat_bounds = [-20.989999771118164, 59.970001220703125]
    nsrdb_lon_bounds = [-179.97999572753906,-22.5]

    sitelist = sitelist[sitelist["latitude"]>min(wtk_lat_bounds)]
    sitelist = sitelist[sitelist["latitude"]>min(nsrdb_lat_bounds)]

    sitelist = sitelist[sitelist["latitude"]<max(wtk_lat_bounds)]
    sitelist = sitelist[sitelist["latitude"]<max(nsrdb_lat_bounds)]

    sitelist = sitelist[sitelist["longitude"]>min(wtk_lon_bounds)]
    sitelist = sitelist[sitelist["longitude"]>min(nsrdb_lon_bounds)]

    sitelist = sitelist[sitelist["longitude"]<max(wtk_lon_bounds)]
    sitelist = sitelist[sitelist["longitude"]<max(nsrdb_lon_bounds)]

    return sitelist
# cdf = get_conus_sitelist()
# []