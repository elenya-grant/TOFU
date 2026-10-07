from tofu import OUTPUT_DIR,DATA_DIR,INPUT_DIR
import os
import pandas as pd
import numpy as np
from tofu.utilities.file_utilities import load_yaml, check_create_folder
from tofu.wind_siting_analysis.wind_tools import calc_setback_distance, calc_buildable_area, make_multi_turbine_layout_square, calc_multi_turbine_min_area

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

# Wind usable area is now supplied per turbine class with reV setbacks already applied, so the
# setback is NOT re-applied here (that would double-count it). The setback calculation uses a
# zeroed turbine config while the real turbine_config.yaml still drives spacing and rated power.
def filter_sitelist_for_wind_turbines(layout_str:str,wind_area_data_filename = "wind_usable_area_by_turbine_class_parcel_10_6_26.csv",obs_id_data_filename = "aggregated_facility_level_site_list_2026_04_23.csv",data_folder=None,conus_only=False):
    """_summary_

    Args:
        layout_str (str): either "5x5" or "3x7" or a new string if a new layout_config file is made
        wind_area_data_filename (str): per-turbine-class wind usable area CSV (one row per PARCEL_LID)
        obs_id_data_filename (str): facility-level CSV used to attach the list of obs_id per parcel
        data_folder (str or None): directory containing both CSVs. If None, uses TOFU DATA_DIR.
        conus_only (bool): keep only CONUS parcels directly extracted from the reV composite
    """
    input_config_dir = os.path.join(str(INPUT_DIR),os.path.dirname(__file__).split("/")[-1])
    output_results_dir = os.path.join(str(OUTPUT_DIR),os.path.dirname(__file__).split("/")[-1])
    check_create_folder(output_results_dir)
    turbine_config_filepath = os.path.join(input_config_dir,"turbine_config.yaml")
    turbine_config_setback_filepath = os.path.join(input_config_dir,"turbine_config_setbackalreadyapplied.yaml")
    layout_config_filepath = os.path.join(input_config_dir,"layout_config_{}.yaml".format(layout_str))
    layout_config = load_yaml(layout_config_filepath)
    turb_config = load_yaml(turbine_config_filepath)
    turb_config_setback = load_yaml(turbine_config_setback_filepath)

    _validate_data_folder(data_folder)
    base_folder = str(DATA_DIR) if data_folder is None else data_folder
    wind_area_filepath = os.path.join(base_folder,wind_area_data_filename)
    obs_id_filepath = os.path.join(base_folder,obs_id_data_filename)

    # per-class usable area column name for each turbine, e.g. wind_usable_sqm_ge_80hh_77rd
    area_col = {turb: "wind_usable_sqm_{}".format(turb_config[turb]["class_suffix"]) for turb in turb_config.keys()}

    base_cols = ["PARCEL_LID","SITE_STATE","parcel_centroid_latitude","parcel_centroid_longitude","flag_below_1ac","flag_area_outlier","wind_usable_source"]
    columns = base_cols + list(area_col.values())
    df = pd.read_csv(wind_area_filepath,usecols=columns)
    df = df.rename(columns={"parcel_centroid_latitude": "latitude", "parcel_centroid_longitude": "longitude", "flag_below_1ac": "under_1_acre"})

    # NOTE: Retain the two sites in AK
    #df = df[~df["flag_area_outlier"].astype(bool)]
    if conus_only:
        df = df[df["wind_usable_source"] == "rev_wind_10_6_26"]
    # drop parcels with no usable area for any turbine class
    df = df[df[list(area_col.values())].max(axis=1) > 0]

    # attach the list of all obs_id sharing each parcel (parcel may hold multiple facilities)
    obs_df = pd.read_csv(obs_id_filepath,usecols=["PARCEL_LID","obs_id"],encoding="ISO-8859-1")
    obs_by_parcel = obs_df.dropna(subset=["obs_id"]).groupby("PARCEL_LID")["obs_id"].apply(list)
    df["obs_id"] = df["PARCEL_LID"].map(obs_by_parcel)

    df = df.reset_index(drop=True)

    for turb in turb_config.keys():
        print("starting turbine: {}".format(turb))
        setback_distance = calc_setback_distance(layout_config["setback_multiplier"],turb_config_setback[turb])
        turb_config[turb].update({"setback_distance":setback_distance})
        if layout_config["setback_shape"]=="square":
            minimum_width = 2*setback_distance
            minimum_length = 2*setback_distance
            minimum_area_m2 = minimum_width*minimum_length #setback_distance**2
        if layout_config["setback_shape"]=="circle":
            minimum_area_m2 = np.pi*(setback_distance**2)
        turb_config[turb].update({"minimum_area_m2":minimum_area_m2})

        turb_area_col = area_col[turb]
        i_usable_sites = df[df[turb_area_col]>minimum_area_m2].index.to_list()
        turb_config[turb].update({"max # sites":len(i_usable_sites)})

        df = calc_buildable_area(land_shape = layout_config["layout_shape"],setback_distance_m = setback_distance,df=df,i_usable_sites = i_usable_sites,turb_name = turb,area_col = turb_area_col)
        
        rotor_diameter = turb_config[turb]["rotor_diameter"]
        
        df["{}: maximum # turbines".format(turb)] = 0
        df["{}: max wind farm capacity [kW]".format(turb)] = 0.0
        df.loc[i_usable_sites,"{}: maximum # turbines".format(turb)] = 1
        df.loc[i_usable_sites,"{}: max wind farm capacity [kW]".format(turb)] = 1*turb_config[turb]["rated_power_kW"]

        max_n_sites = 0
        for site_idx in i_usable_sites:
            n_turbs = make_multi_turbine_layout_square(layout_config,rotor_diameter,df.loc[site_idx,"{}: buildable land area [m^2]".format(turb)],make_layout=False,plot_layout=False)
            if n_turbs>1:
                df.loc[site_idx,"{}: maximum # turbines".format(turb)] = n_turbs
                df.loc[site_idx,"{}: max wind farm capacity [kW]".format(turb)] = n_turbs*turb_config[turb]["rated_power_kW"]
                max_n_sites +=1
        turb_config[turb].update({"max # sites multi-turb":max_n_sites})
    turb_config_output_filename = "turb_info-{}-{}x{}_spacing.csv".format(layout_config["layout_shape"],layout_config["row_spacing"],layout_config["column_spacing"])
    turb_config_output_filepath = os.path.join(output_results_dir,turb_config_output_filename)
    pd.DataFrame(turb_config).to_csv(turb_config_output_filepath)

    # raw per-class usable area columns are now captured in each turbine's buildable-area column
    # (setback = 0, so buildable == usable); drop them and the fully-filtered outlier flag.
    df = df.drop(columns=list(area_col.values()) + ["flag_area_outlier"])

    sitelist_output_filename = "full_sitelist_wind-{}-{}x{}_spacing.csv".format(layout_config["layout_shape"],layout_config["row_spacing"],layout_config["column_spacing"])
    sitelist_output_filepath = os.path.join(output_results_dir,sitelist_output_filename)
    df.to_csv(sitelist_output_filepath)

    # Note: the legacy `wind_exclusion` column has been retired in the new sitelist
    # data, so the previous "nonexclusions_sitelist_*" output is no longer produced.
    print("done!")
    print("wrote output sitelist file to {}".format(sitelist_output_filepath))
    print("wrote turbine summary info file to {}".format(turb_config_output_filepath))
    return df, turb_config