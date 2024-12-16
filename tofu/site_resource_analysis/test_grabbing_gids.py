from tofu.site_resource_analysis.get_resource_gids import get_wtk_site_gids,get_nsrdb_site_gids
from tofu.site_resource_analysis.filter_sitelist_for_conus import get_conus_sitelist,filter_sitelist_for_bounds
from datetime import datetime
import pandas as pd

buggy_lats = [74.6368,59.719368,60.652 ]
buggy_lons = [-35.144363, -154.89761, -151.028]
buggy_df = pd.DataFrame({"latitude":buggy_lats,"longitude":buggy_lons})

n_sites = 10
resource_year = 2012

site_df = get_conus_sitelist(data_folder= "/projects/iedo00onsite/data/pnnl_parcel_land_coverage_data/updated_9_27_2024")
site_df = filter_sitelist_for_bounds(site_df)
print("{} sites".format(len(site_df)))
print("starting...")
start_time = datetime.now()

drop_cols = [k for k in site_df.columns.to_list() if k!= "latitude"]
drop_cols = [k for k in drop_cols if k!="longitude"]
subset_df = site_df.iloc[:n_sites]
subset_df = subset_df.drop(columns=drop_cols)
subset_df = pd.concat([buggy_df,subset_df],axis=0).reset_index(drop=True)


wtk_lat_lons,wtk_gids = get_wtk_site_gids(subset_df,resource_year)
print("completed WTK site gids for {} sites".format(n_sites))
print("wtk gids: {}".format(wtk_gids))

nsrdb_lat_lons,nsrdb_gids = get_nsrdb_site_gids(subset_df,resource_year)
print("completed NSRDB site gids for {} sites".format(n_sites))
print("nsrdb gids: {}".format(nsrdb_gids))

print(f"done! ellapsed time: {datetime.now() - start_time}")