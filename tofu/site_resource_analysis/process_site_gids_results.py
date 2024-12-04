import os
import numpy as np
import pandas as pd
from tofu.utilities.file_utilities import load_yaml, dump_pickle
from tofu import INPUT_DIR
import dill

def get_unique_gids(gid_df,unq_gid_filename):
    unique_wtk_gids = np.unique(gid_df["WTK gid"].to_list())
    unique_nsrdb_gids = np.unique(gid_df["NSDRB gid"].to_list())
    unique_gids = {"WTK gids":unique_wtk_gids,"NSRDB gids":unique_nsrdb_gids}
    dump_pickle(unq_gid_filename,unique_gids)
    print("dumped unique gids into {}".format(unq_gid_filename))

def combine_files(folder,filename_list,output_fname):
    res_df = pd.DataFrame()
    for f in filename_list:
        filename = os.path.join(folder,f)
        df = pd.read_pickle(filename)
        res_df = pd.concat([res_df,df],axis=0)
    output_fpath = os.path.join(folder,output_fname)
    res_df.to_pickle(output_fpath)
    print("saved aggregated result files to {}".format(output_fpath))
    return res_df

if __name__ == "__main__":
    input_folder = os.path.join(str(INPUT_DIR),os.path.dirname(__file__).split("/")[-1])
    input_filename = "run_config.yaml"
    input_filepath = os.path.join(input_folder,input_filename)
    input_config = load_yaml(input_filepath)

    result_folder = input_config["gid_run"]["output_folder"]
    result_file_desc = "site_gids--_"
    result_file_type = ".pkl"
    exlude_result_desc = input_config["gid_run"]["final_gid_sitelist"]
    output_filename = input_config["gid_run"]["final_gid_sitelist"]
    
    unq_gid_filepath = os.path.join(result_folder,input_config["gid_run"]["unq_gid_filename"])
    
    files = os.listdir(result_folder)
    files = [f for f in files if result_file_type in f]
    files = [f for f in files if result_file_desc in f]
    files = [f for f in files if exlude_result_desc not in f]
    files = [f for f in files if f!=output_filename]
    
    if len(files)>0:
        res_gid_df = combine_files(result_folder,files,output_filename)
        get_unique_gids(res_gid_df,unq_gid_filepath)
        print("done")