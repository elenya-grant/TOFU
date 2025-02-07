from tofu.site_resource_analysis.wind_resource import WindResource
import os
from tofu.utilities.file_utilities import dump_pickle

def download_wind_resource_for_single_site(
    sitelist,
    match_id,
    output_dir,
    resource_year=2012,
    lat_string = "parcel_latitude",
    lon_string="parcel_longitude", 
    hub_height_str = "hub_height"
    ):
    lat = sitelist.loc[match_id][lat_string]
    lon = sitelist.loc[match_id][lon_string]
    gid = sitelist.loc[match_id]["WTK gid"]
    hub_height = sitelist.loc[match_id][hub_height_str]

    output_filename = f"{match_id}-{resource_year}-{hub_height}m.pkl"
    output_filepath = os.path.join(output_dir,output_filename)
    wind_rsrc = WindResource(lat,lon,resource_year,hub_height,region="conus",site_gid=gid)
    
    dump_pickle(output_filepath,wind_rsrc.data)
