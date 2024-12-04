
# link: https://nrel.github.io/rex/_autosummary/rex.resource_extraction.resource_extraction.ResourceX.html#rex.resource_extraction.resource_extraction.ResourceX.lat_lon_gid
# from rex.resource_extraction import ResourceX
# ResourceX.lat_lon_gid(lat_lon)

import numpy as np
# latitude = 34.0
# longitude = -127.8
# lat_lon = (latitude, longitude)
# latitudes = [34,35,36,37,38]
# longitudes = [101,102,103,104,105]
# lat_lon = [(lat,lon) for lat,lon in zip(latitudes,longitudes)]
# if not isinstance(lat_lon, np.ndarray):
#     lat_lon = np.array(lat_lon, dtype=np.float32)

# if len(lat_lon.shape) == 1:
#     lat_lon = np.expand_dims(lat_lon, axis=0)
[]
from rex import WindX
from rex import NSRDBX

def get_nsrdb_site_gids(site_df,resource_year,verbose = True):
    nsrdb_file = '/datasets/NSRDB/deprecated_v3/nsrdb_{year}.h5'.format(year=resource_year)
    lats = site_df["latitude"].to_list()
    lons = site_df["longitude"].to_list()
    lat_lon_pairs = [(lat,lon) for lat,lon in zip(lats,lons)]
    with NSRDBX(nsrdb_file, hsds=False) as f:
        gid_list = f.lat_lon_gid(lat_lon_pairs)
    return lat_lon_pairs,gid_list


def get_wtk_site_gids(site_df,resource_year,verbose = True):
    wtk_file = '/datasets/WIND/conus/v1.0.0/wtk_conus_{year}.h5'.format(year=resource_year)
    lats = site_df["latitude"].to_list()
    lons = site_df["longitude"].to_list()
    lat_lon_pairs = [(lat,lon) for lat,lon in zip(lats,lons)]
    with WindX(wtk_file, hsds=False) as f:
        gid_list = f.lat_lon_gid(lat_lon_pairs)
    return lat_lon_pairs,gid_list
