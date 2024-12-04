import pandas as pd
import yaml
import os
import time
import copy
import sys
from mpi4py import MPI
from datetime import datetime
import logging
import faulthandler
from tofu.utilities.file_utilities import check_create_folder,load_yaml
faulthandler.enable()
from tofu.site_resource_analysis.get_resource_gids import get_wtk_site_gids,get_nsrdb_site_gids
from tofu import INPUT_DIR
from tofu.site_resource_analysis.filter_sitelist_for_conus import get_conus_sitelist
from tofu.utilities.tofu_loggers import tofu_logger as tofu_log
def get_gids(site_list,site_idxs,output_filename_base,resource_year):
    w_lat_lons, wtk_gid_list = get_wtk_site_gids(site_list.loc[site_idxs],resource_year)
    s_lat_lons,nsrdb_gid_list = get_nsrdb_site_gids(site_list.loc[site_idxs],resource_year)
    gid_list = pd.DataFrame({"WTK gid":wtk_gid_list,"NSRDB gid":nsrdb_gid_list,"WTK Lat/Lon":w_lat_lons,"NSRDB Lat/Lon":s_lat_lons})
    df = pd.concat([site_list.loc[site_idxs],gid_list],axis=1)
    df.to_pickle(output_filename_base + ".pkl")


start_time = datetime.now()

comm = MPI.COMM_WORLD
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()

def main(sitelist,inputs,verbose = False):
    """Main function
    Basic MPI job for embarrassingly paraller job:
    read data for multiple sites(gids) from one WTK .h5 file
    compute somthing (windspeed min, max, mean) for each site(gid)
    write results to .csv file for each site(gid)
    each rank will get about equal number of sites(gids) to process
    """

    ### input
    tofu_log.info(f"START TIME: {start_time}")
    output_filepath_base_base,rsrc_year = inputs
    site_idxs = sitelist.index
    if rank == 0:
        print(" i'm rank {}:".format(rank))
        ################################ split site_idx's
        s_list = site_idxs.tolist()
        # check if number of ranks <= number of tasks
        if size > len(s_list):
            print(
                "number of scenarios {} < number of ranks {}, abborting...".format(
                    len(s_list), size
                )
            )
            sys.exit()

        # split them into chunks (number of chunks = number of ranks)
        chunk_size = len(s_list) // size

        remainder_size = len(s_list) % size

        s_list_chunks = [
            s_list[i : i + chunk_size] for i in range(0, size * chunk_size, chunk_size)
        ]
        # distribute remainder to chunks
        for i in range(-remainder_size, 0):
            s_list_chunks[i].append(s_list[i])
        if verbose:
            print(f"\n s_list_chunks {s_list_chunks}")
        tofu_log.info(f"s_list_chunks {s_list_chunks}")
    else:
        s_list_chunks = None

    ### scatter
    s_list_chunks = comm.scatter(s_list_chunks, root=0)
    if verbose:
        print(f"\n rank {rank} has sites {s_list_chunks} to process")
    tofu_log.info(f"rank {rank} has sites {s_list_chunks} to process")
   
    get_gids(sitelist,s_list_chunks,output_filepath_base_base + f"_{rank}",rsrc_year)
    if verbose:
        print(f"rank {rank}: ellapsed time: {datetime.now() - start_time}")
    tofu_log.info(f"rank {rank}: ellapsed time: {datetime.now() - start_time}")


if __name__ == "__main__":
    # if len(sys.argv)<4:
    #     year = 2013
    #     output_dir = "/scratch/egrant/TOFU/output/site_resource_analysis/site_gid_list"
    #     check_create_folder(output_dir)

    # else:
    #     year = int(sys.argv[1])
    #     output_dir = sys.argv[2]
    #     check_create_folder(output_dir)
    input_folder = os.path.join(str(INPUT_DIR),os.path.dirname(__file__).split("/")[-1])
    input_filename = "run_config.yaml"
    input_filepath = os.path.join(input_folder,input_filename)
    input_config = load_yaml(input_filepath)
    
    output_dir = input_config["gid_run"]["output_folder"]
    check_create_folder(output_dir)
    year = input_config["resource_data"]["resource_year"]

    output_filepath_base_desc = os.path.join(output_dir,"site_gids--")
    conus_sites = get_conus_sitelist(data_folder=input_config["sitelist"]["directory"],sitelist_data_filename=input_config["sitelist"]["filename"])
    inpt = [output_filepath_base_desc,year]
    main(conus_sites,inpt)