from tofu.wind_siting_analysis.filter_sitelist_for_turbine_siting import filter_sitelist_for_wind_turbines
from tofu.wind_siting_analysis.clean_wind_turbine_sitelist import clean_filtered_sitelist_files,make_sorted_sitelist
from tofu import OUTPUT_DIR
import os
"""
1. make sure to place a siteslist_data_filename in the TOFU/data/ folder
2. input files for this script are in TOFU/input/wind_siting_analysis/
3. output files for this script are written to TOFU/outputs/wind_siting_analysis/
"""
turbine_layout_dspacing_cases = [[5,5],[3,7]]
layout_types = ["{}x{}".format(d[0],d[1]) for d in turbine_layout_dspacing_cases]
r_spacings = [d[0] for d in turbine_layout_dspacing_cases]
c_spacings = [d[1] for d in turbine_layout_dspacing_cases]
run_filtering = True
if run_filtering:
    for layout_str in layout_types:
        filter_sitelist_for_wind_turbines(layout_str,sitelist_data_filename = "aggregated_facility_level_site_list_2026_04_23.csv")

output_dir = os.path.join(str(OUTPUT_DIR),os.path.dirname(__file__).split("/")[-1])
# all sites
print("cleaning sitelist for all sites...")
clean_filtered_sitelist_files(shape = "square", row_spacings = r_spacings, col_spacings = c_spacings, make_summary = True,use_all_sites=True)
print("making sorted sitelist")
make_sorted_sitelist(output_dir,make_sorted_list = True,clean_sorted_list = False,row_spacings = r_spacings,col_spacings = c_spacings,shape = "square",use_full_sitelist=True)
print("cleaning sorted sitelist")
make_sorted_sitelist(output_dir,make_sorted_list = False,clean_sorted_list = True,row_spacings = r_spacings,col_spacings = c_spacings,shape = "square",use_full_sitelist=True)

# only sites without wind exclusions - airport, DoD, etc.
# These things below will likely be redundant since an update to the new sitelist is that if the site has any exclusions,
# then the usable wind area is set to 0, so the filtering above should have already removed these sites. But just in case, we can run this too.
# print("cleaning sitelist for non exclusion sites...")
# clean_filtered_sitelist_files(shape = "square", row_spacings = r_spacings, col_spacings = c_spacings, make_summary = True,use_all_sites=False)
# print("making sorted sitelist")
# make_sorted_sitelist(output_dir,make_sorted_list = True,clean_sorted_list = False,row_spacings = r_spacings,col_spacings = c_spacings,shape = "square",use_full_sitelist=False)
# print("cleaning sorted sitelist")
# make_sorted_sitelist(output_dir,make_sorted_list = False,clean_sorted_list = True,row_spacings = r_spacings,col_spacings = c_spacings,shape = "square",use_full_sitelist=False)
