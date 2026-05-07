import pandas as pd
import os
import numpy as np
from tofu import DATA_DIR,OUTPUT_DIR,INPUT_DIR
from tofu.utilities.file_utilities import load_yaml,check_create_folder
from tofu.site_resource_analysis.filter_sitelist_for_conus import  get_conus_sitelist

# Load the GID-run config so we read the aggregated GID file from the same folder
# `process_site_gids_results.py` writes to.
_gid_run_cfg = load_yaml(os.path.join(str(INPUT_DIR), "site_resource_analysis", "run_config.yaml"))["gid_run"]

sitelist_dir = "/projects/iedo00onsite/onsite-energy-analysis/data/pnnl_parcel_land_coverage_data/updated_4_10_2026"
manuf_sites = get_conus_sitelist(data_folder=sitelist_dir,sitelist_data_filename="aggregated_facility_level_site_list_2026_04_23.csv")
manuf_sites = manuf_sites[manuf_sites["under_1_acre"]==False]
manuf_site_ids = manuf_sites["PARCEL_LID"].to_list()

layout = "5x5"
wind_sitelist_dir = os.path.join(str(OUTPUT_DIR),"wind_siting_analysis")
wind_sitelist_filename = f"best_turb_fullsitelist_wind-square-{layout}_spacing.pkl"
best_turb_per_site = pd.read_pickle(os.path.join(wind_sitelist_dir,wind_sitelist_filename))
best_turb_per_site = best_turb_per_site[best_turb_per_site["under_1_acre"]==False]
#best_turb_per_site = best_turb_per_site[best_turb_per_site["wind_exclusion"]==False]
best_turb_per_site = best_turb_per_site.dropna(axis=0,how="any",subset=["PARCEL_LID","latitude","longitude"])
turb_unique_cols = [k for k in best_turb_per_site.columns.to_list() if k not in manuf_sites.columns.to_list()]
# wind_sitelist_ids = best_turb_per_site["MatchID"].to_list()

turbine_config_filepath = os.path.join(str(INPUT_DIR),"wind_siting_analysis","turbine_config.yaml")
turbine_config = load_yaml(turbine_config_filepath)
turbine_to_hubht = {k:v["hub_height"] for k,v in turbine_config.items()}

site_gid_fpath = os.path.join(_gid_run_cfg["output_folder"], _gid_run_cfg["final_gid_sitelist"])
site_gids = pd.read_pickle(site_gid_fpath)
site_gids = site_gids.dropna(axis=0,how="any",subset=["PARCEL_LID","WTK gid","NSRDB gid","latitude","longitude"])
gid_unique_cols = [k for k in site_gids.columns.to_list() if k not in manuf_sites.columns.to_list()]
# site_gid_ids = site_gids["MatchID"].to_list()

common_ids = sorted(
    set(manuf_site_ids)
    & set(best_turb_per_site["PARCEL_LID"])
    & set(site_gids["PARCEL_LID"])
)
n_dropped = len(set(manuf_site_ids)) - len(common_ids)
if n_dropped:
    print(f"WARNING: {n_dropped} PARCEL_LIDs from manuf_sites not found in turbine or GID data — skipping them.")

t1 = best_turb_per_site.set_index(keys=["PARCEL_LID"]).loc[common_ids][turb_unique_cols]
t2 = site_gids.set_index(keys=["PARCEL_LID"]).loc[common_ids][gid_unique_cols]
final_df = pd.concat([manuf_sites.set_index(keys=["PARCEL_LID"]).loc[common_ids],t1,t2],axis=1)

final_df["hub_height"] = None
for turbine,hub_height in turbine_to_hubht.items():
    ii = final_df[final_df["best turbine"]==turbine].index.to_list()
    final_df.loc[ii,"hub_height"] = hub_height

n_before = len(final_df)
final_df = final_df.dropna(subset=["hub_height"])
n_dropped_hh = n_before - len(final_df)
if n_dropped_hh:
    print(f"WARNING: Dropped {n_dropped_hh} sites with hub_height=None.")
final_df["hub_height"] = final_df["hub_height"].astype(int)
final_df["WTK gid"] = final_df["WTK gid"].astype(int)

# Add a column per turbine hub height so Phase 4 can download all 5
for turbine_name, hh in turbine_to_hubht.items():
    col_name = f"hub_height_{int(hh)}"
    final_df[col_name] = int(hh)

final_df = final_df.drop_duplicates()
final_data_fname = f"wind_sites_for_resource_download_{layout}.pkl"
final_data_dir = os.path.join(str(OUTPUT_DIR),os.path.dirname(__file__).split("/")[-1])
check_create_folder(final_data_dir)
final_data_fpath = os.path.join(final_data_dir,final_data_fname)
final_df.to_pickle(final_data_fpath)
final_df.to_csv(final_data_fpath.replace(".pkl",".csv"))

# --- Phase B: Create wind_site_list_3x7_spacing.csv ---
# Read the sorted turbine sitelist (ranked turbine_0 through turbine_4)
sorted_sitelist_fname = f"sorted_turb_fullsitelist_wind-square-{layout}_spacing.pkl"
sorted_sitelist_fpath = os.path.join(wind_sitelist_dir, sorted_sitelist_fname)
sorted_df = pd.read_pickle(sorted_sitelist_fpath)
sorted_df = sorted_df[sorted_df["under_1_acre"] == False]
sorted_df = sorted_df.dropna(axis=0, how="any", subset=["PARCEL_LID", "latitude", "longitude"])

# Columns unique to the sorted sitelist (turbine rankings)
sorted_unique_cols = [k for k in sorted_df.columns.to_list() if k not in manuf_sites.columns.to_list()]

# Intersect with manuf_sites and site_gids
sorted_common_ids = sorted(
    set(manuf_site_ids)
    & set(sorted_df["PARCEL_LID"])
    & set(site_gids["PARCEL_LID"])
)

s1 = sorted_df.set_index(keys=["PARCEL_LID"]).loc[sorted_common_ids][sorted_unique_cols]
s2 = site_gids.set_index(keys=["PARCEL_LID"]).loc[sorted_common_ids][gid_unique_cols]
wind_site_list_df = pd.concat(
    [manuf_sites.set_index(keys=["PARCEL_LID"]).loc[sorted_common_ids], s1, s2],
    axis=1,
)
wind_site_list_df["WTK gid"] = wind_site_list_df["WTK gid"].astype(int)
wind_site_list_df = wind_site_list_df.drop_duplicates()

wind_site_list_fname = f"wind_site_list_{layout}_spacing.csv"
wind_site_list_fpath = os.path.join(final_data_dir, wind_site_list_fname)
wind_site_list_df.to_csv(wind_site_list_fpath)
wind_site_list_df.to_pickle(wind_site_list_fpath.replace(".csv", ".pkl"))
print(f"Saved wind_site_list to {wind_site_list_fpath} ({len(wind_site_list_df)} sites)")
