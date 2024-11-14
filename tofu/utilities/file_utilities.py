import yaml
import os
from pathlib import Path
import pandas as pd
import geopandas as gpd
import h5pyd

class Loader(yaml.SafeLoader):

    def __init__(self, stream):

        self._root = os.path.split(stream.name)[0]

        super().__init__(stream)

    def include(self, node):

        filename = os.path.join(self._root, self.construct_scalar(node))

        with open(filename, 'r') as f:
            return yaml.load(f, self.__class__)


Loader.add_constructor('!include', Loader.include)

def load_yaml(filename, loader=Loader):
    with open(filename) as fid:
        return yaml.load(fid, loader)

def write_yaml(filename,data):
    if not '.yaml' in filename:
        filename = filename +'.yaml'

    with open('filename', 'w') as file:
        yaml.dump(data, file,sort_keys=False)
    return filename

def check_create_folder(filepath):
    if not os.path.isdir(filepath):
        os.makedirs(filepath)
        already_exists = False
    else:
        already_exists = True

    return already_exists

def load_file(filepath,file_kwargs={}):
    filename = filepath.split("/")[-1]
    file_extension = filename.split(".")[-1]
    if os.stat(filepath).st_size > 0:
    
        if file_extension=='xlsx':
            if file_kwargs == {}:
                pd.read_excel(filepath)
            else:
                file = pd.read_excel(filepath,**file_kwargs)
        elif file_extension == 'csv':
            if file_kwargs == {}:
                file = pd.read_csv(filepath)
            else:
                file = pd.read_csv(filepath,**file_kwargs)
        elif file_extension == 'yaml':
            file = load_yaml(filepath)
        elif file_extension == 'yml':
            file = load_yaml(filepath)
        elif file_extension == 'pkl':
            if file_kwargs == {}:
                file = pd.read_pickle(filepath)
            else:
                file = pd.read_pickle(filepath,**file_kwargs)
        elif file_extension == '':
            if file_kwargs == {}:
                file = pd.read_pickle(filepath)
            else:
                file = pd.read_pickle(filepath,**file_kwargs)
        elif file_extension == 'shp':
            file = gpd.read_file(filepath,crs=4326)
        elif file_extension == 'h5':
            file = h5pyd.File(filepath,'r')
    else:
        print("File is empty")
        file = pd.DataFrame()
    return file

# def convert_dataframe_to_gpd(df):
#     geometry_avail = any(col for col in df.columns.to_list() if col.lower() == 'geometry')

def convert_dataframe_to_gpd(df,lat_str = None,lon_str=None):
    if lat_str is None:
        lat_str = [col for col in df.columns.to_list() if 'lat' in col.lower()]
        lat_str = lat_str[0]
    if lon_str is None:
        lon_str = [col for col in df.columns.to_list() if 'lon' in col.lower()]
        lon_str = lon_str[0]
    df["latitude"] = pd.to_numeric(df[lat_str], errors='coerce')
    df["longitude"] = pd.to_numeric(df[lon_str], errors='coerce')
    df_geo = gpd.GeoDataFrame(df, 
                geometry = gpd.points_from_xy(
                        df.longitude,
                        df.latitude
                ))
    return df_geo

def find_data_shapefile(files):
    data_file = [file for file in files if file.split(".")[-1]=="shp"]
    if len(data_file)==0:
        data_file = [file for file in files if file.split(".")[-1]=="csv"]
    return data_file[0]