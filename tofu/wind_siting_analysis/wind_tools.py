import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import Polygon

def calc_setback_distance(setback_mutliplier, turb_info:dict):
    tip_height = (turb_info["rotor_diameter"]/2) + turb_info["hub_height"]
    setback_distance = tip_height*setback_mutliplier
    return setback_distance #m

def calc_buildable_area_circle(setback_distance_m,wind_ground_area_m2):
    A_tot = np.array(wind_ground_area_m2)
    r_tot = np.sqrt(A_tot/np.pi)

    r_buildable = r_tot - setback_distance_m
    A_buildable = np.pi*(r_buildable**2)
    perimeter_buildable = 2*np.pi*r_buildable
    return r_buildable,A_buildable,perimeter_buildable

def calc_buildable_area_square(setback_distance_m,wind_ground_area_m2):
    A_tot = np.array(wind_ground_area_m2)
    width = np.sqrt(A_tot)
    length = A_tot/width

    width_buildable = width - (2*setback_distance_m)
    length_buildable = length - (2*setback_distance_m)
    A_buildable = width_buildable*length_buildable
    perimeter_buildable = (2*width_buildable) + (2*length_buildable)
    return width_buildable,A_buildable,perimeter_buildable

def calc_buildable_area(land_shape,setback_distance_m,df,i_usable_sites,turb_name):
    wind_area = df.loc[i_usable_sites,"wind_ground_area"].to_list()
    if land_shape == "circle":
        r,area,p = calc_buildable_area_circle(setback_distance_m,wind_area)
        df["{}: buildable land radius [m]".format(turb_name)] = np.zeros(len(df))
        df.loc[i_usable_sites,"{}: buildable land radius [m]".format(turb_name)] = r
    if land_shape == "square":
        r,area,p = calc_buildable_area_square(setback_distance_m,wind_area)
        df["{}: buildable land width [m]".format(turb_name)] = np.zeros(len(df))
        df.loc[i_usable_sites,"{}: buildable land width [m]".format(turb_name)] = r
    
    df["{}: buildable land area [m^2]".format(turb_name)] = np.zeros(len(df))
    df["{}: buildable land perimeter [m]".format(turb_name)] = np.zeros(len(df))
    df.loc[i_usable_sites,"{}: buildable land perimeter [m]".format(turb_name)] = p
    df.loc[i_usable_sites,"{}: buildable land area [m^2]".format(turb_name)] = area
    return df

def make_multi_turbine_layout_square(layout_config,rotor_diameter,a_buildable_max:float,make_layout=False,plot_layout=False):
    spacing_x = min([layout_config["row_spacing"]*rotor_diameter,layout_config["column_spacing"]*rotor_diameter])
    spacing_y = max([layout_config["row_spacing"]*rotor_diameter,layout_config["column_spacing"]*rotor_diameter])
    
    area_width = np.sqrt(a_buildable_max)
    area_length = a_buildable_max/area_width
    x_buffer = spacing_x/10
    y_buffer = spacing_y/10
    x_locs = np.arange(0,area_width+x_buffer,spacing_x)
    y_locs = np.arange(0,area_length+y_buffer,spacing_y)
    

    if max(x_locs)>area_width:
        x_locs = [x for x in x_locs if x<=area_width]
    if max(y_locs)>area_length:
        y_locs = [y for y in y_locs if y<=area_length]

    n_turbs = len(x_locs)*len(y_locs)
    if make_layout or plot_layout:
        turbine_layout_x = np.zeros(len(x_locs)*len(y_locs))
        turbine_layout_y = np.zeros(len(x_locs)*len(y_locs))
        turb_i = 0
        site_shape = Polygon(((0,0),(0,area_length),(area_width,area_length),(area_width,0)))
        for x in x_locs:
            for y in y_locs:
                turbine_layout_x[turb_i] = x
                turbine_layout_y[turb_i] = y
                turb_i +=1

    if plot_layout:
        plt.scatter(turbine_layout_x,turbine_layout_y)
        plt.plot(*site_shape.exterior.xy,color="red")
    if make_layout:
        locs = [(x,y) for x,y in zip(turbine_layout_x,turbine_layout_y)]
        return n_turbs,locs
    else:
        return n_turbs
        
def calc_multi_turbine_min_area(layout_config,rotor_diameter):
    spacing_x = min([layout_config["row_spacing"]*rotor_diameter,layout_config["column_spacing"]*rotor_diameter])
    # spacing_y = max([layout_config["row_spacing"]*rotor_diameter,layout_config["column_spacing"]*rotor_diameter])
    min_area_square = spacing_x*spacing_x
    return min_area_square