
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
from rex.utilities.exceptions import ResourceValueError
from tofu.utilities.tofu_loggers import tofu_logger as tofu_log
def handle_resource(lat_lon_pairs,error_string):
    print("esg-note: handling exception for gid issues")
    buggy_lat_lons = error_string.split("(")[1].split(")")[0]
    buggy_lat_lons = buggy_lat_lons.split("\n")
    buggy_lat_lons = [b.strip().strip("[").strip("]").strip() for b in buggy_lat_lons]
    buggy_lats = [float(b.split(" ")[0]) for b in buggy_lat_lons]
    buggy_lons = [float(b.split(" ")[-1]) for b in buggy_lat_lons]
    for blat,blon in zip(buggy_lats,buggy_lons):
        lat_lon_pairs = [k for k in lat_lon_pairs if k!= (blat,blon)]
    tofu_log.info("buggy lat/lons are: {}".format(buggy_lat_lons))
    return lat_lon_pairs

def get_nsrdb_site_gids(site_df,resource_year,verbose = True):
    nsrdb_file = '/datasets/NSRDB/current/nsrdb_{year}.h5'.format(year=resource_year)
    lats = site_df["latitude"].to_list()
    lons = site_df["longitude"].to_list()
    lat_lon_pairs = [(lat,lon) for lat,lon in zip(lats,lons)]
    with NSRDBX(nsrdb_file, hsds=False) as f:
        try:
            gid_list = f.lat_lon_gid(lat_lon_pairs)
        except ResourceValueError as expt:
            error_msg = expt.args
            e_msg = error_msg[0]

            # print("nsrdb error message: {} \n".format(error_msg))
            
            lat_lon_pairs = handle_resource(lat_lon_pairs,e_msg)
            if len(lat_lon_pairs)>0:
                gid_list = f.lat_lon_gid(lat_lon_pairs,check_lat_lon=False)
            else:
                gid_list = []
        # gid_list = f.lat_lon_gid(lat_lon_pairs)
    return lat_lon_pairs,gid_list


def get_wtk_site_gids(site_df,resource_year,verbose = True):
    wtk_file = '/datasets/WIND/conus/v1.0.0/wtk_conus_{year}.h5'.format(year=resource_year)
    lats = site_df["latitude"].to_list()
    lons = site_df["longitude"].to_list()
    lat_lon_pairs = [(lat,lon) for lat,lon in zip(lats,lons)]
    with WindX(wtk_file, hsds=False) as f:
        try:
            gid_list = f.lat_lon_gid(lat_lon_pairs)
        except ResourceValueError as expt:

            error_msg = expt.args
            e_msg = error_msg[0]
            lat_lon_pairs = handle_resource(lat_lon_pairs,e_msg)
            if len(lat_lon_pairs)>0:
                gid_list = f.lat_lon_gid(lat_lon_pairs, check_lat_lon=False)
            else:
                gid_list = []
            # print("resource value error args:")
            # error_msg = expt.args
            # e_msg = error_msg[0]
            # print("wtk error is type {}".format(type(error_msg[0])))
            # print(error_msg)
            # print("\n")
            # gid_list = []
    return lat_lon_pairs,gid_list
