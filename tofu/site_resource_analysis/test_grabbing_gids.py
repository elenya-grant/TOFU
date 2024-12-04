from tofu.site_resource_analysis.get_resource_gids import get_wtk_site_gids,get_nsrdb_site_gids
from tofu.site_resource_analysis.filter_sitelist_for_conus import get_conus_sitelist
from datetime import datetime

n_sites = 10
resource_year = 2012

site_df = get_conus_sitelist(data_folder=None)

print("starting...")
start_time = datetime.now()

wtk_lat_lons,wtk_gids = get_wtk_site_gids(site_df.iloc[:n_sites],resource_year)
print("completed WTK site gids for {} sites".format(n_sites))
print("wtk gids: {}".format(wtk_gids))

nsrdb_lat_lons,nsrdb_gids = get_nsrdb_site_gids(site_df.iloc[:n_sites],resource_year)
print("completed NSRDB site gids for {} sites".format(n_sites))
print("nsrdb gids: {}".format(nsrdb_gids))

print(f"done! ellapsed time: {datetime.now() - start_time}")