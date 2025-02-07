from tofu import OUTPUT_DIR
import pandas as pd
import os
import sys
from mpi4py import MPI
from datetime import datetime
import logging
import faulthandler
# from tofu.utilities.file_utilities import check_create_folder,load_yaml
faulthandler.enable()
import copy
from tofu.utilities.tofu_loggers import tofu_logger as tofu_log
from tofu.wind_resource.get_save_wind_resource_data import download_wind_resource_for_single_site

def do_something(sitelist,match_id,input_cnfg):
    if "hub_ht_str" in input_cnfg.keys():
        if isinstance(input_cnfg["hubt_ht_str"],list):
            for hh_str in input_cnfg["hub_ht_str"]:
                download_wind_resource_for_single_site(
                    sitelist,
                    match_id,
                    input_cnfg["output_dir"],
                    resource_year=input_cnfg["resource_year"],
                    lat_string=input_cnfg["lat_string"],
                    lon_string=input_cnfg["lon_string"],
                    hub_height_str = hh_str
                )
        else:
            download_wind_resource_for_single_site(
                sitelist,
                match_id,
                input_cnfg["output_dir"],
                resource_year=input_cnfg["resource_year"],
                lat_string=input_cnfg["lat_string"],
                lon_string=input_cnfg["lon_string"],
                hub_height_str = input_cnfg["hubt_ht_str"],
            )
    else:
        download_wind_resource_for_single_site(
                sitelist,
                match_id,
                input_cnfg["output_dir"],
                resource_year=input_cnfg["resource_year"],
                lat_string=input_cnfg["lat_string"],
                lon_string=input_cnfg["lon_string"]
        )

    

start_time = datetime.now()

comm = MPI.COMM_WORLD
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()


def main(sitelist,input_info,verbose = False):
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
    tofu_log.debug(f"rank {rank} has sites {s_list_chunks} to process")

    # ### run sites in serial
    for i, matchid in enumerate(s_list_chunks):
        # time.sleep(rank * 5)
        if verbose:
            print(f"rank {rank} now processing its sites in serial: site gid {matchid}")
        tofu_log.debug(f"rank {rank} now processing its sites in serial: Site {matchid}")
        inputs_copied = copy.deepcopy(input_info)
        do_something(sitelist,matchid,inputs_copied)
    if verbose:
        print(f"rank {rank}: ellapsed time: {datetime.now() - start_time}")
    tofu_log.debug(f"rank {rank}: ellapsed time: {datetime.now() - start_time}")

if __name__=="__main__":
    layout = "3x7"
    wind_sitelist_fname = f"wind_sites_for_non_original_resource_download_{layout}.pkl"
    # wind_sitelist_fname = f"wind_sites_for_resource_download_{layout}.pkl"
    wind_sitelist_dir = os.path.join(str(OUTPUT_DIR),os.path.dirname(__file__).split("/")[-1])
    wind_sitelist_fpath = os.path.join(wind_sitelist_dir,wind_sitelist_fname)
    sitelist = pd.read_pickle(wind_sitelist_fpath)

    
    wind_resource_dir = "/projects/iedo00onsite/data/wind_resource_data"
    input_dict = {
        "output_dir":wind_resource_dir,
        "resource_year":2012,
        "lat_string":"parcel_latitude",
        "lon_string":"parcel_longitude",
        "hub_ht_str": ["turbine_1 hub_height","turbine_2 hub_height"],
        }
    main(sitelist,input_dict)
    
    # check_create_folder(wind_resource_dir)
