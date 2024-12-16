import os
import numpy as np
import pandas as pd
from tofu.utilities.file_utilities import load_yaml, dump_pickle
from tofu import INPUT_DIR
import dill

def get_unique_gids(gid_df,unq_gid_filename):
    unique_wtk_gids = np.unique(gid_df["WTK gid"].to_list())
    unique_nsrdb_gids = np.unique(gid_df["NSRDB gid"].to_list())
    unique_gids = {"WTK gids":unique_wtk_gids,"NSRDB gids":unique_nsrdb_gids}
    dump_pickle(unq_gid_filename,unique_gids)
    print("{} unique wtk gids and {} unique nsrdb gids".format(len(unique_wtk_gids),len(unique_nsrdb_gids)))
    print("dumped unique gids into {}".format(unq_gid_filename))

def combine_files(folder,filename_list,output_fname):
    res_df = pd.DataFrame()
    info_a_cols = ["parcel_lid","MatchID","state","latitude","longitude","parcel_latitude","parcel_longitude","wind_ground_area","under_1_acre"]
    info_b_cols = ["WTK gid","NSRDB gid","WTK Lat/Lon","NSRDB Lat/Lon"]
    for f in filename_list:
        filename = os.path.join(folder,f)
        df = pd.read_pickle(filename)
        split_idx = int(len(df)/2)
        info_a = df.iloc[:split_idx]
        info_b = df.iloc[split_idx:]
        
        a_dropcols = [k for k in info_a.columns.to_list() if k not in info_a_cols]
        b_dropcols = [k for k in info_b.columns.to_list() if k not in info_b_cols]
        info_b.index = info_a.index.to_list()

        info_a = info_a.drop(columns=a_dropcols)
        info_b = info_b.drop(columns=b_dropcols)

        df_new = pd.concat([info_a,info_b],axis=1)

        res_df = pd.concat([res_df,df_new],axis=0)
    output_fpath = os.path.join(folder,output_fname)
    res_df.to_pickle(output_fpath)
    print("results has {} sites".format(len(res_df)))
    print("saved aggregated result files to {}".format(output_fpath))
    return res_df

def remove_files(folder,remove_file_desc,keep_file_desc):
    all_files = os.listdir(folder)
    remove_files = [f for f in all_files if keep_file_desc not in f]
    remove_files = [f for f in remove_files if remove_file_desc in f]
    print("going to delete {} files".format(len(remove_files)))
    for file in remove_files:
        delete_this_filepath = os.path.join(folder,file)
        os.remove(delete_this_filepath)
    print("done deleting files")



if __name__ == "__main__":
    input_folder = os.path.join(str(INPUT_DIR),os.path.dirname(__file__).split("/")[-1])
    input_filename = "run_config.yaml"
    input_filepath = os.path.join(input_folder,input_filename)
    input_config = load_yaml(input_filepath)

    result_folder = input_config["gid_run"]["output_folder"]
    result_file_desc = "site_gids--_"
    result_file_type = ".pkl"
    exlude_result_desc = input_config["gid_run"]["final_gid_sitelist"]
    # output_filename = input_config["gid_run"]["final_gid_sitelist"]
    
    exlude_result_desc = "site_gid_list.pkl"
    output_filename = "site_gid_list.pkl"
    unq_gid_filepath = os.path.join(result_folder,input_config["gid_run"]["unique_gid_list"])
    # --- BELOW IS HOW TO REMOVE FILES ---
    # remove_files(result_folder,result_file_desc,"site_resource")
    # remove_files(result_folder,"solar_site_resource--_","solar_site_resource.p")
    # remove_files(result_folder,"wind_site_resource--_","wind_site_resource.p")
    
    # ---- BELOW IS TO COMBINE ----
    # input_folder = os.path.join(str(INPUT_DIR),os.path.dirname(__file__).split("/")[-1])
    # input_filename = "run_config.yaml"
    # input_filepath = os.path.join(input_folder,input_filename)
    # input_config = load_yaml(input_filepath)

    # result_folder = input_config["gid_run"]["output_folder"]
    # result_file_desc = "site_gids--_"
    # result_file_type = ".pkl"
    # exlude_result_desc = input_config["gid_run"]["final_gid_sitelist"]
    # # output_filename = input_config["gid_run"]["final_gid_sitelist"]
    
    # exlude_result_desc = "site_gid_list.pkl"
    # output_filename = "site_gid_list.pkl"
    # unq_gid_filepath = os.path.join(result_folder,input_config["gid_run"]["unique_gid_list"])
    
    # files = os.listdir(result_folder)
    # files = [f for f in files if result_file_type in f]
    # files = [f for f in files if result_file_desc in f]
    # files = [f for f in files if exlude_result_desc not in f]
    # files = [f for f in files if input_config["gid_run"]["unique_gid_list"] not in f]
    # files = [f for f in files if f!=output_filename]
    
    # if len(files)>0:
    #     res_gid_df = combine_files(result_folder,files,output_filename)
    #     get_unique_gids(res_gid_df,unq_gid_filepath)
    #     print("done")