from tofu import OUTPUT_DIR,DATA_DIR,INPUT_DIR
import os
import pandas as pd
import numpy as np
from tofu.utilities.file_utilities import load_yaml, check_create_folder

def load_sorted_file_remove_nonwind_sites(r_space,c_space,output_dir,shape="square",use_all_sites=True):
    if use_all_sites:
        summary_filename = "sorted_turb_fullsitelist_wind-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
    else:
        summary_filename = "sorted_turb_nonexclusionsitelist_wind-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
    # summary_filepath = os.path.join(output_dir,summary_filename)
    summary_filepath_pkl = os.path.join(output_dir,summary_filename.replace(".csv",".pkl"))

    df = pd.read_pickle(summary_filepath_pkl)

    geo_cols = ["parcel_lid","MatchID","latitude","longitude","state","parcel_latitude","parcel_longitude","wind_ground_area","under_1_acre"]
    turb_cols = [k for k in df.columns.to_list() if k not in geo_cols]
    df = df.dropna(axis='index',how='all',subset=turb_cols)

    if use_all_sites:
        clean_filename = "wind_fullsitelist_sorted-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
    else:
        clean_filename = "wind_nonexclusionsitelist_sorted-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)

    clean_filepath_csv = os.path.join(output_dir,clean_filename)
    clean_filepath_pkl = os.path.join(output_dir,clean_filename.replace(".csv",".pkl"))
    []
    df.to_pickle(clean_filepath_pkl)
    df.to_csv(clean_filepath_csv)
    print("done")

def clean_filtered_sitelist_files(shape = "square", row_spacings = [5,3], col_spacings = [5,7], make_summary = True,use_all_sites=True):
    output_dir = os.path.join(str(OUTPUT_DIR),os.path.dirname(__file__).split("/")[-1])
    if make_summary:
        # make summary file
        for r_space,c_space in zip(row_spacings,col_spacings):
            if use_all_sites:
                sitelist_filename = "full_sitelist_wind-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
            else:
                sitelist_filename = "nonexclusions_sitelist_wind-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
            sitelist_filepath = os.path.join(output_dir,sitelist_filename)

            turb_config_output_filename = "turb_info-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
            turb_config_output_filepath = os.path.join(output_dir,turb_config_output_filename)

            site_df = pd.read_csv(sitelist_filepath,index_col = "Unnamed: 0")
            turb_df = pd.read_csv(turb_config_output_filepath,index_col = "Unnamed: 0")
            turb_names = turb_df.columns.to_list()
            site_turb_cols = [c for c in site_df.columns.to_list() if any(t in c for t in turb_names)]
            replace_dict_info = dict(zip(site_turb_cols,np.zeros(len(site_turb_cols))))
            site_df = site_df.replace(to_replace = replace_dict_info,value = np.nan)
            n_cols_pr_turb = int(len(site_turb_cols)/len(turb_names))
            turb_data_col_desc = [k.split(": ")[-1] for k in site_turb_cols[:n_cols_pr_turb]]
            site_df = site_df.dropna(axis='index',how='all',subset=site_turb_cols)

            n_sites_pr_turb = [int(k) for k in turb_df.loc["max # sites"].to_list()]
            n_sites_sorted, turb_name_sorted = (list(t) for t in zip(*sorted(zip(n_sites_pr_turb, turb_names))))
            
            max_capac_colnames = ["{}: max wind farm capacity [kW]".format(t) for t in turb_names]
            # for turb_name in turb_name_sorted:
            #     turb_idx = site_df["{}: maximum # turbines".format(turb_name)].dropna().index.to_list()
            site_df = site_df.fillna(value = 0)
            site_df[max_capac_colnames]
            max_wind_capacity_turb_per_site = site_df[max_capac_colnames].idxmax(axis="columns")
            max_wind_capacity_per_site = site_df[max_capac_colnames].max(axis="columns")
            max_wind_capacity_per_site.name = "Maximum Wind Capacity [kW]"

            max_wind_capacity_turb_per_site = max_wind_capacity_turb_per_site.replace(to_replace = max_capac_colnames,value=turb_names)
            max_wind_capacity_turb_per_site.name="best turbine"

            site_df = pd.concat([site_df,max_wind_capacity_turb_per_site,max_wind_capacity_per_site],axis=1)
            site_df['# turbines'] = None
            site_df['buildable land area [m^2]'] = None
            for s in site_df.index.to_list():
                site_df.loc[s,'buildable land area [m^2]'] = site_df.loc[s,"{}: buildable land area [m^2]".format(site_df.loc[s,"best turbine"])]
                site_df.loc[s,'# turbines'] = site_df.loc[s,"{}: maximum # turbines".format(site_df.loc[s,"best turbine"])]
            
            site_df = site_df.drop(columns=site_turb_cols)
            if use_all_sites:
                summary_filename = "best_turb_fullsitelist_wind-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
            else:
                summary_filename = "best_turb_nonexclusionsitelist_wind-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
            summary_filepath = os.path.join(output_dir,summary_filename)
            summary_filepath_pkl = os.path.join(output_dir,summary_filename.replace(".csv",".pkl"))
            site_df.to_csv(summary_filepath)
            site_df.to_pickle(summary_filepath_pkl)
            print("done")
            print("saved sitelist summary file to {}".format(summary_filepath))



def remove_option_tier_turbs_from_site(site_df,best_turb_df):

    for i in best_turb_df.index.to_list():
        best_turb_name = best_turb_df.loc[i]["best turbine"]
        turb_cols = [k for k in site_df.columns.to_list() if best_turb_name in k]
        site_df.loc[i,turb_cols] = 0
    return site_df

def find_best_turb_per_site(site_df,turb_df):

    
    turb_names = turb_df.columns.to_list()
    site_turb_cols = [c for c in site_df.columns.to_list() if any(t in c for t in turb_names)]
    replace_dict_info = dict(zip(site_turb_cols,np.zeros(len(site_turb_cols))))
    site_df = site_df.replace(to_replace = replace_dict_info,value = np.nan)
    site_df = site_df.dropna(axis='index',how='all',subset=site_turb_cols)

    max_capac_colnames = ["{}: max wind farm capacity [kW]".format(t) for t in turb_names]
    
    site_df = site_df.fillna(value = 0)
    max_wind_capacity_turb_per_site = site_df[max_capac_colnames].idxmax(axis="columns")
    max_wind_capacity_per_site = site_df[max_capac_colnames].max(axis="columns")
    max_wind_capacity_per_site.name = "Maximum Wind Capacity [kW]"

    max_wind_capacity_turb_per_site = max_wind_capacity_turb_per_site.replace(to_replace = max_capac_colnames,value=turb_names)
    max_wind_capacity_turb_per_site.name="best turbine"

    site_df = pd.concat([site_df,max_wind_capacity_turb_per_site,max_wind_capacity_per_site],axis=1)
    site_df['# turbines'] = None
    site_df['buildable land area [m^2]'] = None
    for s in site_df.index.to_list():
        site_df.loc[s,'buildable land area [m^2]'] = site_df.loc[s,"{}: buildable land area [m^2]".format(site_df.loc[s,"best turbine"])]
        site_df.loc[s,'# turbines'] = site_df.loc[s,"{}: maximum # turbines".format(site_df.loc[s,"best turbine"])]
    site_df = site_df.drop(columns=site_turb_cols)
   
    return site_df

def make_sorted_sitelist(output_dir,make_sorted_list = False,clean_sorted_list = True,row_spacings = [5,3],col_spacings = [5,7],shape = "square",use_full_sitelist=True):
    

    if clean_sorted_list:
        for r_space,c_space in zip(row_spacings,col_spacings):
            load_sorted_file_remove_nonwind_sites(r_space,c_space,output_dir,shape="square",use_all_sites=use_full_sitelist)
    
    if make_sorted_list:
        site_columns = ["parcel_lid","MatchID","latitude","longitude","state","parcel_latitude","parcel_longitude","wind_ground_area","under_1_acre","best turbine"]
        geo_cols = ["parcel_lid","MatchID","latitude","longitude","state","parcel_latitude","parcel_longitude","wind_ground_area","under_1_acre"]
        for r_space,c_space in zip(row_spacings,col_spacings):
            if use_full_sitelist:
                sitelist_filename = "full_sitelist_wind-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
            else:
                sitelist_filename = "nonexclusions_sitelist_wind-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
            sitelist_filepath = os.path.join(output_dir,sitelist_filename)

            turb_config_output_filename = "turb_info-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
            turb_config_output_filepath = os.path.join(output_dir,turb_config_output_filename)

            site_df = pd.read_csv(sitelist_filepath,index_col = "Unnamed: 0")
            turb_df = pd.read_csv(turb_config_output_filepath,index_col = "Unnamed: 0")
            # turb_list = turb_df.columns.to_list()
            max_options = len(turb_df.columns.to_list())
            final_df = pd.DataFrame()
            final_df = pd.concat([site_df[geo_cols],final_df])
            for i in range(max_options):
                print("running {} of {}".format(i,max_options))
                if i==0:
                    best_turb_df = find_best_turb_per_site(site_df,turb_df)
                    site_df_new = remove_option_tier_turbs_from_site(site_df,best_turb_df)
                else:
                    best_turb_df = find_best_turb_per_site(site_df_new,turb_df)
                    site_df_new = remove_option_tier_turbs_from_site(site_df_new,best_turb_df)

                
                added_cols = [k for k in best_turb_df.columns.to_list() if k not in site_columns]
                new_cols = ["turbine_{}: {}".format(i,k) for k in added_cols]
                best_turb_df = best_turb_df.rename(columns={"best turbine":"turbine_{}".format(i)})
                best_turb_df = best_turb_df.rename(columns=dict(zip(added_cols,new_cols)))
                best_turb_df = best_turb_df.drop(columns = geo_cols)
                final_df = pd.concat([final_df,best_turb_df],axis=1)
                []

            if use_full_sitelist:
                summary_filename = "sorted_turb_fullsitelist_wind-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
            else:
                summary_filename = "sorted_turb_nonexclusionsitelist_wind-{}-{}x{}_spacing.csv".format(shape,r_space,c_space)
            summary_filepath = os.path.join(output_dir,summary_filename)
            summary_filepath_pkl = os.path.join(output_dir,summary_filename.replace(".csv",".pkl"))
            final_df.to_csv(summary_filepath)
            final_df.to_pickle(summary_filepath_pkl)
            print("done")
            []
