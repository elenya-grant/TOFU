from tofu import OUTPUT_DIR,DATA_DIR,INPUT_DIR
import os
import pandas as pd
import numpy as np
from tofu.utilities.file_utilities import load_yaml, check_create_folder
from tofu.wind_siting_analysis.wind_tools import calc_setback_distance, calc_buildable_area, make_multi_turbine_layout_square, calc_multi_turbine_min_area

def filter_sitelist_for_wind_turbines(layout_str:str,sitelist_data_filename = "LC_facility_parcels_NREL_9_27.csv"):
    """_summary_

    Args:
        layout_str (str): either "5x5" or "3x7" or a new string if a new layout_config file is made
    """
    input_config_dir = os.path.join(str(INPUT_DIR),os.path.dirname(__file__).split("/")[-1])
    output_results_dir = os.path.join(str(OUTPUT_DIR),os.path.dirname(__file__).split("/")[-1])
    check_create_folder(output_results_dir)
    turbine_config_filepath = os.path.join(input_config_dir,"turbine_config.yaml")
    layout_config_filepath = os.path.join(input_config_dir,"layout_config_{}.yaml".format(layout_str))
    layout_config = load_yaml(layout_config_filepath)
    turb_config = load_yaml(turbine_config_filepath)

    sitelist_filepath = os.path.join(str(DATA_DIR),sitelist_data_filename)
    columns = ["parcel_lid","MatchID","latitude","longitude","parcel_latitude","parcel_longitude","wind_ground_area","under_1_acre"]
    df  = pd.read_csv(sitelist_filepath,usecols=columns,encoding = "ISO-8859-1")
    df = df[df["wind_ground_area"]>0]

    for turb in turb_config.keys():
        print("starting turbine: {}".format(turb))
        setback_distance = calc_setback_distance(layout_config["setback_multiplier"],turb_config[turb])
        turb_config[turb].update({"setback_distance":setback_distance})
        if layout_config["setback_shape"]=="square":
            minimum_width = 2*setback_distance
            minimum_length = 2*setback_distance
            minimum_area_m2 = minimum_width*minimum_length #setback_distance**2
        if layout_config["setback_shape"]=="circle":
            minimum_area_m2 = np.pi*(setback_distance**2)
        turb_config[turb].update({"minimum_area_m2":minimum_area_m2})
        n_sites_with_min_area = len(df[df["wind_ground_area"]>=minimum_area_m2])
        turb_config[turb].update({"max # sites":n_sites_with_min_area})
        
        i_usable_sites = df[df["wind_ground_area"]>=minimum_area_m2].index.to_list()

        df = calc_buildable_area(land_shape = layout_config["layout_shape"],setback_distance_m = setback_distance,df=df,i_usable_sites = i_usable_sites,turb_name = turb)
        
        rotor_diameter = turb_config[turb]["rotor_diameter"]
        
        df["{}: maximum # turbines".format(turb)] = 0
        df["{}: max wind farm capacity [kW]".format(turb)] = 0
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

    sitelist_output_filename = "full_sitelist_wind-{}-{}x{}_spacing.csv".format(layout_config["layout_shape"],layout_config["row_spacing"],layout_config["column_spacing"])
    sitelist_output_filepath = os.path.join(output_results_dir,sitelist_output_filename)
    df.to_csv(sitelist_output_filepath)
    print("done!")
    print("wrote output sitelist file to {}".format(sitelist_output_filepath))
    print("wrote turbine summary info file to {}".format(turb_config_output_filepath))
    return df, turb_config