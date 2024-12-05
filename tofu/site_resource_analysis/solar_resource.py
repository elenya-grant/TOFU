from rex import NSRDBX
from rex.sam_resource import SAMResource
import numpy as np 
import pandas as pd
# import pandas as pd
NSRDB_DEP = "/kfs2/datasets/NSRDB/deprecated_v3/nsrdb_"
NSRDB_NEW = "/kfs2/datasets/NSRDB/current/nsrdb_"
#conus    deprecated_v3  himawari  meteosat  mts1  NSRDB.md                     philippines
#current  full_disc      india     msg       mts2  nsrdb_v3_to_current_map.csv  vietnam
#current/nsrdb_tmy-2023.h5
# Pull Solar Resource Data directly from NSRDB on HPC
# To be called instead of SolarResource from hopp.simulation.technologies.resource
class SolarResource:

    def __init__(self,lat,lon,resource_year,site_gid = None):
        """
        Input:
        lat (float): site latitude
        lon (float): site longitude
        resource_year (int): year to get resource data for
        
        Output: self.data
            dictionary:
                tz: float
                elev: float
                lat: float
                lon: float
                year: list of floats
                month: list of floats
                day: list of floats
                hour: list of floats
                minute: list of floats
                dn: list of floats
                df: list of floats
                gh: list of floats
                wspd: list of floats
                tdry: list of floats
                pres: list of floats
                tdew: list of floats
        """
        # NOTE: self.data must be compatible with PVWatts.SolarResource.solar_resource_data to https://nrel-pysam.readthedocs.io/en/main/modules/Pvwattsv8.html#PySAM.Pvwattsv8.Pvwattsv8.SolarResource
        self.latitude = lat
        self.longitude = lon
        self.year = resource_year
        self.site_gid = site_gid

        # Pull data from HPC NSRDB dataset
        self.extract_resource()

        # Set solar resource data into SAM/PySAM digestible format
        self.format_data()

        # Define final dictionary
        self.data = {'tz' :     self.time_zone,
                     'elev' :   self.elevation,
                     'lat' :    self.nsrdb_latitude,
                     'lon' :    self.nsrdb_longitude,
                     'year' :   self.year_arr,
                     'month' :  self.month_arr,
                     'day' :    self.day_arr,
                     'hour' :   self.hour_arr,
                     'minute' : self.minute_arr,
                     'dn' :     self.dni_arr,
                     'df' :     self.dhi_arr,
                     'gh' :     self.ghi_arr,
                     'wspd' :   self.wspd_arr,
                     'tdry' :   self.tdry_arr,
                     'pres' :   self.pres_arr,
                     'tdew' :   self.tdew_arr
                    }

    def extract_resource(self):
        # Define file to download from
        # NOTE: HOPP is currently calling an old deprecated version of PSM v3.1 which corresponds to /api/nsrdb/v2/solar/psm3-download
        # nsrdb_file = '/datasets/NSRDB/deprecated_v3/nsrdb_{year}.h5'.format(year=self.year)
        
        # NOTE: Current version of PSM v3.2.2 which corresponds to /api/nsrdb/v2/solar/psm3-2-2-download 
        nsrdb_file = '/datasets/NSRDB/current/nsrdb_{year}.h5'.format(year=self.year)
        # nsrdb_tmy-2022.h5

        # Open file with rex NSRDBX object
        with NSRDBX(nsrdb_file, hsds=False) as f:
            # get gid of location closest to given lat/lon coordinates
            if self.site_gid is None:
                site_gid = f.lat_lon_gid((self.latitude,self.longitude))
                self.site_gid = site_gid

            # extract timezone, elevation, latitude and longitude from meta dataset with gid
            self.time_zone = f.meta['timezone'].iloc[self.site_gid]
            self.elevation = f.meta['elevation'].iloc[self.site_gid]
            self.nsrdb_latitude = f.meta['latitude'].iloc[self.site_gid]
            self.nsrdb_longitude = f.meta['longitude'].iloc[self.site_gid]
            
            # extract remaining datapoints: year, month, day, hour, minute, dn, df, gh, wspd,tdry, pres, tdew
            # NOTE: datasets have readings at 0 and 30 minutes each hour, HOPP/SAM workflow requires only 30 minute reading values -> filter 0 minute readings with [1::2]
            # NOTE: datasets are not auto shifted by timezone offset -> wrap extraction in SAMResource.roll_timeseries(input_array, timezone, #steps in an hour=1) to roll timezones
            # NOTE: solar_resource.py code references solar_zenith_angle and RH = relative_humidity but I couldn't find them actually being utilized. Captured them below just in case.
            self.year_arr = f.time_index.year.values[1::2]
            self.month_arr = f.time_index.month.values[1::2]
            self.day_arr = f.time_index.day.values[1::2]
            self.hour_arr = f.time_index.hour.values[1::2]
            self.minute_arr = f.time_index.minute.values[1::2]
            self.dni_arr = SAMResource.roll_timeseries((f['dni', :, site_gid][1::2]), self.time_zone, 1)
            self.dhi_arr = SAMResource.roll_timeseries((f['dhi', :, site_gid][1::2]), self.time_zone, 1)
            self.ghi_arr = SAMResource.roll_timeseries((f['ghi', :, site_gid][1::2]), self.time_zone, 1)
            self.wspd_arr = SAMResource.roll_timeseries((f['wind_speed', :, site_gid][1::2]), self.time_zone, 1)
            self.tdry_arr = SAMResource.roll_timeseries((f['air_temperature', :, site_gid][1::2]), self.time_zone, 1)
            # self.relative_humidity_arr = SAMResource.roll_timeseries((f['relative_humidity', :, site_gid][1::2]), self.time_zone, 1)
            # self.solar_zenith_arr = SAMResource.roll_timeseries((f['solar_zenith_angle', :, site_gid][1::2]), self.time_zone, 1)
            self.pres_arr = SAMResource.roll_timeseries((f['surface_pressure', :, site_gid][1::2]), self.time_zone, 1)
            self.tdew_arr = SAMResource.roll_timeseries((f['dew_point', :, site_gid][1::2]), self.time_zone, 1)
    
    def format_data(self):
        # Remove data from feb29 on leap years
        if (self.year % 4) == 0:
            feb29 = np.arange(1416,1440)
            self.year_arr = np.delete(self.year_arr, feb29)
            self.month_arr = np.delete(self.month_arr, feb29)
            self.day_arr = np.delete(self.day_arr, feb29)
            self.hour_arr = np.delete(self.hour_arr, feb29)
            self.minute_arr = np.delete(self.minute_arr, feb29)
            self.dni_arr = np.delete(self.dni_arr, feb29)
            self.dhi_arr = np.delete(self.dhi_arr, feb29)
            self.ghi_arr = np.delete(self.ghi_arr, feb29)
            self.wspd_arr = np.delete(self.wspd_arr, feb29)
            self.tdry_arr = np.delete(self.tdry_arr, feb29)
            # self.relative_humidity_arr = np.delete(self.relative_humidity_arr, feb29)
            # self.solar_zenith_arr = np.delete(self.solar_zenith_arr, feb29)
            self.pres_arr = np.delete(self.pres_arr, feb29)
            self.tdew_arr = np.delete(self.tdew_arr, feb29)

        # round to desired precision and convert to desired data type
        # NOTE: unsure if SAM/PySAM is sensitive to data types and decimal precision. If not sensitive, can remove .astype() and round() to increase computational efficiency
        self.time_zone = float(self.time_zone)
        self.elevation = round(float(self.elevation), 0)
        self.nsrdb_latitude = round(float(self.nsrdb_latitude), 2)
        self.nsrdb_longitude = round(float(self.nsrdb_longitude),2)
        self.year_arr = list(self.year_arr.astype(float, copy=False))
        self.month_arr = list(self.month_arr.astype(float, copy=False))
        self.day_arr = list(self.day_arr.astype(float, copy=False))
        self.hour_arr = list(self.hour_arr.astype(float, copy=False))
        self.minute_arr = list(self.minute_arr.astype(float, copy=False))
        self.dni_arr = list(self.dni_arr.astype(float, copy=False))
        self.dhi_arr = list(self.dhi_arr.astype(float, copy=False))
        self.ghi_arr = list(self.ghi_arr.astype(float, copy=False))
        self.wspd_arr = list(self.wspd_arr.astype(float, copy=False))
        self.tdry_arr = list(self.tdry_arr.astype(float, copy=False))
        # self.relative_humidity_arr = list(np.round(self.relative_humidity_arr, decimals=1))
        # self.solar_zenith_angle_arr = list(np.round(self.solar_zenith_angle_arr, decimals=1))
        self.pres_arr = list(self.pres_arr.astype(float, copy=False))
        self.tdew_arr = list(self.tdew_arr.astype(float, copy=False))

    def summarize_annual_resource(self,return_site_lat_lon=True):
        if return_site_lat_lon:
            keys = ["site latitude","site longitude","resource year"]
            vals = [self.latitude,self.longitude,self.year]
        else:
            keys = ["resource year"]
            vals = [self.year]

        keys += ["site_gid","elevation","time zone","nsrdb latitude","nsrdb longitude"]
        vals += [self.site_gid,self.elevation,self.time_zone,self.nsrdb_latitude,self.nsrdb_longitude]
        
        keys += ["mean ghi","max ghi","min ghi","median ghi"]
        vals += [np.mean(self.ghi_arr),np.max(self.ghi_arr),np.min(self.ghi_arr),np.median(self.ghi_arr)]

        return pd.DataFrame(dict(zip(keys,vals)),index=[self.site_gid])