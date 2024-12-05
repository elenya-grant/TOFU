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
from tofu import INPUT_DIR
from tofu.site_resource_analysis.filter_sitelist_for_conus import get_conus_sitelist
from tofu.utilities.file_utilities import load_yaml, load_pickle
from tofu.utilities.tofu_loggers import tofu_logger as tofu_log
from tofu.site_resource_analysis.wind_resource import WindResource
from tofu.site_resource_analysis.solar_resource import SolarResource
import numpy as np

def get_site_wind_resource(wtk_gid,output_filename_base,resource_year,hub_height):
    tofu_log.info(f"Running wind resource gid(s): {wtk_gid}")
    if isinstance(wtk_gid,list):
        wind_rsrc = pd.DataFrame()
        for wind_gid in wtk_gid:
            w = WindResource(lat=40.00,lon = -105.00,resource_year = resource_year,hub_height = hub_height,region="conus",site_gid = wind_gid)
            wtemp = w.summarize_annual_resource()
            wind_rsrc = pd.concat([wtemp,wind_rsrc],axis=0)
    elif isinstance(wtk_gid,int):
        w = WindResource(lat=40.00,lon = -105.00,resource_year = resource_year,hub_height = hub_height,region="conus",site_gid = wtk_gid)
        wind_rsrc = w.summarize_annual_resource()
    wind_rsrc.to_pickle(output_filename_base + ".pkl")
    tofu_log.info(f"Saved wind resource gid(s) to: {output_filename_base}")

def get_site_solar_resource(nsrdb_gid,output_filename_base,resource_year):
    tofu_log.info(f"Running solar resource gid(s): {nsrdb_gid}")
    if isinstance(nsrdb_gid,list):
        pv_rsrc = pd.DataFrame()
        for solar_gid in nsrdb_gid:
            s = SolarResource(lat=40.00,lon = -105.00,resource_year = resource_year,site_gid = solar_gid)
            stemp = s.summarize_annual_resource()
            pv_rsrc = pd.concat([stemp,pv_rsrc],axis=0)
        
    elif isinstance(nsrdb_gid,int):
        s = SolarResource(lat=40.00,lon = -105.00,resource_year = resource_year,site_gid = nsrdb_gid)
        pv_rsrc = s.summarize_annual_resource()
    pv_rsrc.to_pickle(output_filename_base + ".pkl")
    tofu_log.info(f"Saved solar resource gid(s) to: {output_filename_base}")



start_time = datetime.now()

comm = MPI.COMM_WORLD
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()

def main(site_idxs,inputs,verbose = False):
    """Main function
    Basic MPI job for embarrassingly paraller job:
    read data for multiple sites(gids) from one WTK .h5 file
    compute somthing (windspeed min, max, mean) for each site(gid)
    write results to .csv file for each site(gid)
    each rank will get about equal number of sites(gids) to process
    """

    ### input
    tofu_log.info(f"START TIME: {start_time}")
    output_filepath_base_base,rsrc_year,resource_type,hh = inputs
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
    
    if resource_type=="wind":
        get_site_wind_resource(s_list_chunks,output_filepath_base_base + f"_{rank}",rsrc_year,hh)
    if resource_type=="solar":
        get_site_solar_resource(s_list_chunks,output_filepath_base_base + f"_{rank}",rsrc_year)
    if verbose:
        print(f"rank {rank}: ellapsed time: {datetime.now() - start_time}")
    tofu_log.info(f"rank {rank}: ellapsed time: {datetime.now() - start_time}")


if __name__ == "__main__":
    if len(sys.argv)<3:
        run_wind = True
        run_solar = False
    else:
        run_wind = bool(sys.argv[1])
        run_solar = bool(sys.argv[2])
    # load input config
    input_folder = os.path.join(str(INPUT_DIR),os.path.dirname(__file__).split("/")[-1])
    input_filename = "run_config.yaml"
    input_filepath = os.path.join(input_folder,input_filename)
    input_config = load_yaml(input_filepath)
    
    result_folder = input_config["gid_run"]["output_folder"]
    unq_gid_filepath = os.path.join(result_folder,input_config["gid_run"]["unique_gid_list"])
    gid_list = load_pickle(unq_gid_filepath)
    check_create_folder(result_folder)
    year = input_config["resource_data"]["resource_year"]

    # conus_sites = get_conus_sitelist(data_folder=input_config["sitelist"]["directory"],sitelist_data_filename=input_config["sitelist"]["filename"])
    # ---- RUN WIND ---
    if run_wind:
        output_filepath_base_desc_wind = os.path.join(result_folder,"wind_site_resource--")
        hub_ht = input_config["resource_data"]["wtk"]["hub_height"]
        wind_inpt = [output_filepath_base_desc_wind,year,"wind",hub_ht]
        wind_gid_list = np.delete(gid_list["WTK gids"],np.argwhere(np.isnan(gid_list["WTK gids"])).flatten())
        wind_gid_list = wind_gid_list.astype(int)
        main(wind_gid_list,wind_inpt)
    # ---- RUN SOLAR ---
    if run_solar:
        output_filepath_base_desc_pv = os.path.join(result_folder,"solar_site_resource--")
        solar_inpt = [output_filepath_base_desc_pv,year,"solar",None]
        solar_gid_list = np.delete(gid_list["NSRDB gids"],np.argwhere(np.isnan(gid_list["NSRDB gids"])).flatten())
        solar_gid_list = solar_gid_list.astype(int)
        main(solar_gid_list,solar_inpt)