from rex import WindX
from rex.sam_resource import SAMResource
import numpy as np
import pandas as pd
# Pull Wind Resource Data directly from WTK on HPC
# /datasets/WIND/Alaska/v2.0.0/<year>/alaska_<year>_<hub_height>m.h5 #year is 2018, 2019, 2020
# /datasets/WIND/Hawaii/Hawaii_<year>.h5 #year is 2000-2019
# /datasets/WIND/Hawaii/<year>/Hawaii_<year>_<hub_height>m.h5

class WindResource:
    
    def __init__(self,lat,lon,resource_year,hub_height,region="conus",site_gid = None):
        """
        INPUTS:
        -------
            lat (float): site latitude
            lon (float): site longitude
            resource_year (int): year to get resource for
                - int: year for resource
                - str: either "tmy" or "tdy"
            hub_height (float or int): hub height of turbine used
            region (str), options include: site region
                - "conus"
                - "alaska"
                - "hawaii"
        RETURNS:
        -------
            self.data: data dictionary in SAM/PySAM format with following key/value pairs
            heights: list of hub-heights to use, same length as "fields"
                ex: [100.0,100.0,100.0,100.0,120.0,120.0,120.0,120.0]
            fields: list of fields per hub-height, fields are indicated with integers ranging from 1-4
                ex: [1,2,3,4,1,2,3,4]
                1: Temperature (Celsius)
                2: Pressure (atm)
                3: Speed (m/s)
                4: Direction (degrees)
            data: embedded list. Should have a length=8760 and each row has a length equal to the length of "fields" and "hub-heights"
                ex: data[0] looks like:
                    [Field 1 at hub-height #1, Field 2 at hub-height #1, Field 3 at hub-height #1, Field 4 at hub-height #1, Field 1 at hub-height #2, Field 2 at hub-height #2, Field 3 at hub-height #2, Field 4 at hub-height #2]
        
        """
        self.data_keys = ["heights","fields","data"] # original variable from Elenya
        self.latitude = lat
        self.longitude = lon
        self.year = resource_year
        self.hub_height = hub_height
        self.allowed_hub_heights_meters = [10, 40, 60, 80, 100, 120, 140, 160, 200]
        self.region = region
        # Determine which hub height files need to be downloaded
        # NOTE: if hub_height is not in allowed list, current HOPP setup downloads data from 2 neighboring heights, ex: if hub_height = 97m, downloads data from 80m and 100m
        # Question: is this required by HOPP or SAM? Rex will automatically interpolates when extracting data ex: if hub_height = 97m, can produce all data at 97m rather than pulling 80m and 100m 
        self.data_hub_heights = self.calculate_heights_to_download()
        
        
        self.site_gid = site_gid
        # Pull data from HPC WIND dataset
        self.extract_resource()

        # Set wind resource data into SAM/PySAM digestible format
        self.format_data() 

        # Define final dictionary
        self.data = {'heights': [float(h) for h in self.data_hub_heights for i in range(4)],
                     'fields':  [1, 2, 3, 4] * len(self.data_hub_heights),
                     'data':    self.combined_data
                    }

    def calculate_heights_to_download(self):
        """
        Given the system hub height, and the available hubheights from WindToolkit,
        determine which heights to download to bracket the hub height
        """
        hub_height_meters = self.hub_height

        # evaluate hub height, determine what heights to download
        heights = [hub_height_meters]
        if hub_height_meters not in self.allowed_hub_heights_meters:
            height_low = self.allowed_hub_heights_meters[0]
            height_high = self.allowed_hub_heights_meters[-1]
            for h in self.allowed_hub_heights_meters:
                if h < hub_height_meters:
                    height_low = h
                elif h > hub_height_meters:
                    height_high = h
                    break
            heights[0] = height_low
            heights.append(height_high)

        return heights

    def extract_resource(self):
        # Define file to download from
        # NOTE: Current setup of files on HPC WINDToolkit v1.0.0 = 2007-2013, v1.1.0 = 2014
        if self.region == "conus":
            if self.year < 2014 and self.year>2006:
                wtk_file = '/datasets/WIND/conus/v1.0.0/wtk_conus_{year}.h5'.format(year=self.year)
            elif self.year == 2014:
                wtk_file = '/datasets/WIND/conus/v1.1.0/wtk_conus_{year}.h5'.format(year=self.year)
            else:
                raise UserWarning("Wind toolkit resources years for CONUS are only available for 2007 - 2014")
        
        elif self.region=="alaska":
            if self.year > 2017 and self.year < 2021:
                wtk_file = '/datasets/WIND/Alaska/v2.0.0/{year}/alaska_{year}_hourly.h5'.format(year=self.year)
            else:
                raise UserWarning("Wind toolkit resources years for Alaska are only available for 2018 - 2020")
        
        elif self.region=="hawaii":
            if self.year > 1999 and self.year < 2020:
                wtk_file = '/datasets/WIND/Hawaii/Hawaii_{year}.h5'.format(year=self.year)
            else:
                raise UserWarning("Wind toolkit resources years for Hawaii are only available for 2000 - 2019")
        
        else:
            raise UserWarning('Invalid region for Wind toolkit - must be either "conus" or "alaska" or "hawaii"')

        # Open file with rex WindX object
        with WindX(wtk_file, hsds=False) as f:
            # get gid of location closest to given lat/lon coordinates and timezone offset
            if self.site_gid is None:
                site_gid = f.lat_lon_gid((self.latitude, self.longitude))
                self.site_gid = site_gid
            time_zone = f.meta['timezone'].iloc[self.site_gid]
            self.wtk_latitude = f.meta['latitude'].iloc[self.site_gid]
            self.wtk_longitude = f.meta['longitude'].iloc[self.site_gid]
            
            # instantiate temp dictionary to hold each attributes dataset
            self.wind_dict = {}
            # loop through hub heights to download, capture datasets
            # NOTE: datasets are not auto shifted by timezone offset -> wrap extraction in SAMResource.roll_timeseries(input_array, timezone, #steps in an hour=1) to roll timezones
            # NOTE: pressure datasets unit = Pa, convert to atm via division by 101325
            for h in self.data_hub_heights:
                self.wind_dict['temperature_{height}m_arr'.format(height=h)] = SAMResource.roll_timeseries((f['temperature_{height}m'.format(height=h), :, self.site_gid]), time_zone, 1)
                self.wind_dict['pressure_{height}m_arr'.format(height=h)] = SAMResource.roll_timeseries((f['pressure_{height}m'.format(height=h), :, self.site_gid]/101325), time_zone, 1)
                self.wind_dict['windspeed_{height}m_arr'.format(height=h)] = SAMResource.roll_timeseries((f['windspeed_{height}m'.format(height=h), :, self.site_gid]), time_zone, 1)
                self.wind_dict['winddirection_{height}m_arr'.format(height=h)] = SAMResource.roll_timeseries((f['winddirection_{height}m'.format(height=h), :, self.site_gid]), time_zone, 1)    
    
    def format_data(self):
        # Remove data from feb29 on leap years
        if (self.year % 4) == 0:
            feb29 = np.arange(1416,1440)
            for key, value in self.wind_dict.items():
                self.wind_dict[key] = np.delete(value, feb29)

        # round to desired precision and concatenate data into format needed for data dictionary
        if len(self.data_hub_heights) == 2:
            # NOTE: Unsure if SAM/PySAM is sensitive to data types ie: floats with long precision vs to 2 or 3 decimals. If not sensitive, can remove following 8 lines of code to increase computational efficiency
            self.wind_dict['temperature_{h}m_arr'.format(h=self.data_hub_heights[0])] = np.round((self.wind_dict['temperature_{h}m_arr'.format(h=self.data_hub_heights[0])]), decimals=1)
            self.wind_dict['pressure_{h}m_arr'.format(h=self.data_hub_heights[0])] = np.round((self.wind_dict['pressure_{h}m_arr'.format(h=self.data_hub_heights[0])]), decimals=2)
            self.wind_dict['windspeed_{h}m_arr'.format(h=self.data_hub_heights[0])] = np.round((self.wind_dict['windspeed_{h}m_arr'.format(h=self.data_hub_heights[0])]), decimals=3)
            self.wind_dict['winddirection_{h}m_arr'.format(h=self.data_hub_heights[0])] = np.round((self.wind_dict['winddirection_{h}m_arr'.format(h=self.data_hub_heights[0])]), decimals=1)
            self.wind_dict['temperature_{h}m_arr'.format(h=self.data_hub_heights[1])] = np.round((self.wind_dict['temperature_{h}m_arr'.format(h=self.data_hub_heights[1])]), decimals=1)
            self.wind_dict['pressure_{h}m_arr'.format(h=self.data_hub_heights[1])] = np.round((self.wind_dict['pressure_{h}m_arr'.format(h=self.data_hub_heights[1])]), decimals=2)
            self.wind_dict['windspeed_{h}m_arr'.format(h=self.data_hub_heights[1])] = np.round((self.wind_dict['windspeed_{h}m_arr'.format(h=self.data_hub_heights[1])]), decimals=3)
            self.wind_dict['winddirection_{h}m_arr'.format(h=self.data_hub_heights[1])] = np.round((self.wind_dict['winddirection_{h}m_arr'.format(h=self.data_hub_heights[1])]), decimals=1)
            # combine all data into one 2D list
            self.combined_data = [list(a) for a in zip(self.wind_dict['temperature_{h}m_arr'.format(h=self.data_hub_heights[0])],
                                                       self.wind_dict['pressure_{h}m_arr'.format(h=self.data_hub_heights[0])],
                                                       self.wind_dict['windspeed_{h}m_arr'.format(h=self.data_hub_heights[0])],
                                                       self.wind_dict['winddirection_{h}m_arr'.format(h=self.data_hub_heights[0])],
                                                       self.wind_dict['temperature_{h}m_arr'.format(h=self.data_hub_heights[1])],
                                                       self.wind_dict['pressure_{h}m_arr'.format(h=self.data_hub_heights[1])],
                                                       self.wind_dict['windspeed_{h}m_arr'.format(h=self.data_hub_heights[1])],
                                                       self.wind_dict['winddirection_{h}m_arr'.format(h=self.data_hub_heights[1])])]

        elif len(self.data_hub_heights) == 1:
            # NOTE: Unsure if SAM/PySAM is sensitive to data types ie: floats with long precision vs to 2 or 3 decimals. If not sensitive, can remove following 4 lines of code to increase computational efficiency
            self.wind_dict['temperature_{h}m_arr'.format(h=self.data_hub_heights[0])] = np.round((self.wind_dict['temperature_{h}m_arr'.format(h=self.data_hub_heights[0])]), decimals=1)
            self.wind_dict['pressure_{h}m_arr'.format(h=self.data_hub_heights[0])] = np.round((self.wind_dict['pressure_{h}m_arr'.format(h=self.data_hub_heights[0])]), decimals=2)
            self.wind_dict['windspeed_{h}m_arr'.format(h=self.data_hub_heights[0])] = np.round((self.wind_dict['windspeed_{h}m_arr'.format(h=self.data_hub_heights[0])]), decimals=3)
            self.wind_dict['winddirection_{h}m_arr'.format(h=self.data_hub_heights[0])] = np.round((self.wind_dict['winddirection_{h}m_arr'.format(h=self.data_hub_heights[0])]), decimals=1)
            # combine all data into one 2D list
            self.combined_data = [list(a) for a in zip(self.wind_dict['temperature_{h}m_arr'.format(h=self.data_hub_heights[0])],
                                                       self.wind_dict['pressure_{h}m_arr'.format(h=self.data_hub_heights[0])],
                                                       self.wind_dict['windspeed_{h}m_arr'.format(h=self.data_hub_heights[0])],
                                                       self.wind_dict['winddirection_{h}m_arr'.format(h=self.data_hub_heights[0])])]
    def summarize_annual_resource(self,return_site_lat_lon = True):
        if return_site_lat_lon:
            keys = ["site latitude","site longitude","resource year"]
            vals = [self.latitude,self.longitude,self.year]
        else:
            keys = ["resource year"]
            vals = [self.year]
            
        keys += ["site_gid","hub_height","wtk latitude","wtk longitude"]
        vals += [self.site_gid,self.hub_height,self.wtk_latitude,self.wtk_longitude]
        
        ws_keys = [k for k in self.wind_dict.keys() if "windspeed" in k]
        for k in ws_keys:
            keys += ["mean {}".format(k.replace("_arr","")),"max {}".format(k.replace("_arr","")),"min {}".format(k.replace("_arr","")),"median {}".format(k.replace("_arr",""))]
            vals += [np.mean(self.wind_dict[k]),np.max(self.wind_dict[k]),np.min(self.wind_dict[k]),np.median(self.wind_dict[k])]

        wd_keys = [k for k in self.wind_dict.keys() if "winddirection" in k]
        for k in wd_keys:
            keys += ["mean {}".format(k.replace("_arr","")),"max {}".format(k.replace("_arr","")),"min {}".format(k.replace("_arr","")),"median {}".format(k.replace("_arr",""))]
            vals += [np.mean(self.wind_dict[k]),np.max(self.wind_dict[k]),np.min(self.wind_dict[k]),np.median(self.wind_dict[k])]
        # keys += ["mean wind direction","max wind direction","min wind direction","median wind direction"]
        return pd.DataFrame(dict(zip(keys,vals)),index=[self.site_gid])