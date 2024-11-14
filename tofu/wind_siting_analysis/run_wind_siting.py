from tofu.wind_siting_analysis.filter_sitelist_for_turbine_siting import filter_sitelist_for_wind_turbines
from tofu.wind_siting_analysis.clean_wind_turbine_sitelist import clean_filtered_sitelist_files

"""
1. make sure to place a siteslist_data_filename in the TOFU/data/ folder
2. input files for this script are in TOFU/input/wind_siting_analysis/
3. output files for this script are written to TOFU/outputs/wind_siting_analysis/
"""
turbine_layout_dspacing_cases = [[5,5],[3,7]]
layout_types = ["{}x{}".format(d[0],d[1]) for d in turbine_layout_dspacing_cases]
r_spacings = [d[0] for d in turbine_layout_dspacing_cases]
c_spacings = [d[1] for d in turbine_layout_dspacing_cases]

for layout_str in layout_types:
    filter_sitelist_for_wind_turbines(layout_str,sitelist_data_filename = "LC_facility_parcels_NREL_9_27.csv")

clean_filtered_sitelist_files(shape = "square", row_spacings = r_spacings, col_spacings = c_spacings, make_summary = True)
