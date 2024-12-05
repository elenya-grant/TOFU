from tofu.site_resource_analysis.wind_resource import WindResource
from tofu.site_resource_analysis.solar_resource import SolarResource
from datetime import datetime
start_time = datetime.now()
print("starting")
resource_year = 2012
hub_height = 110
wind_gid = 127810
solar_gid = 108229
w = WindResource(lat=40.00,lon = -105.00,resource_year = resource_year,hub_height = hub_height,region="conus",site_gid = wind_gid)
wtemp = w.summarize_annual_resource()
print(f"done with wind resource! ellapsed time: {datetime.now() - start_time}")
s = SolarResource(lat=40.00,lon = -105.00,resource_year = resource_year,site_gid = solar_gid)
stemp = s.summarize_annual_resource()
print(f"done with solar resource! total ellapsed time: {datetime.now() - start_time}")