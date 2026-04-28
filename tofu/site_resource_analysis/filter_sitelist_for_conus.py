import pandas as pd
from tofu import OUTPUT_DIR,DATA_DIR,INPUT_DIR
import os
import numpy as np
from tofu.utilities.file_utilities import load_yaml, check_create_folder

# Allowed root for caller-provided overrides of `data_folder`. The current
# absolute default is kept for backwards compatibility, but any override must
# resolve under this directory tree.
_ALLOWED_DATA_ROOT = "/projects/iedo00onsite"

def _validate_data_folder(data_folder):
    if data_folder is None:
        return
    abs_path = os.path.realpath(data_folder)
    allowed_root = os.path.realpath(_ALLOWED_DATA_ROOT)
    if os.path.commonpath([abs_path, allowed_root]) != allowed_root:
        raise ValueError(
            f"data_folder override '{data_folder}' must resolve under '{_ALLOWED_DATA_ROOT}'"
        )

def get_conus_sitelist(data_folder = "/projects/iedo00onsite/onsite-energy-analysis/data/pnnl_parcel_land_coverage_data/updated_4_10_2026",sitelist_data_filename = "facility_level_sitelist_2026_04_22.csv"):
    non_conus = ['HI','VI','MP','GU','AK','AS','PR']

    _validate_data_folder(data_folder)
    if data_folder is None:
        sitelist_filepath = os.path.join(str(DATA_DIR),sitelist_data_filename)
    else:
        sitelist_filepath = os.path.join(data_folder,sitelist_data_filename)
    columns = ["PARCEL_LID","obs_id","SITE_STATE","best_lat","best_lon","usable_wind_sqm","under_1_acre"]
    df  = pd.read_csv(sitelist_filepath,usecols=columns,encoding = "ISO-8859-1")

    for n in non_conus:
        df = df[df["SITE_STATE"] != n]
    df = df.dropna(axis=0,how='any',subset=["best_lat","best_lon","obs_id"])
    df = df.rename(columns={"best_lat": "latitude", "best_lon": "longitude"})
    # lat_min, lon_min, lat_max, lon_max
    return df

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
