# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 16:43:52 2024

@author: SabreKartik
"""
import math

def added_resis(DSS, wave_dir, BF, wind_dir, ship_dir):
    """
    Parameters
    ----------
    DSS : Doouglas Sea State
        DESCRIPTION.
    wave_dir : Global wave direction in degrees
        DESCRIPTION.
    BF : Beaufort Scale of wind between 0-360
        DESCRIPTION.
    wind_dir : Global wind direction in degrees between 0-360
        DESCRIPTION.
    ship_dir : Global direction in degrees of the vessel between 0-360
        DESCRIPTION.

    Returns
    -------
    per : Added Resistance percentage
        DESCRIPTION.
    """
    wave_dir = (3.414/180.0)*(((ship_dir - wave_dir)**(2.0))**(0.5)) #relative wave dir
    wind_dir = (3.414/180.0)*(((ship_dir - wind_dir)**(2.0))**(0.5)) #relative wind dir
    
    if wave_dir <= 3.1414/4.0 :
        fac1 = (DSS**1.1)*math.cos(wave_dir)
    else:
        fac1 = 0
    if wind_dir <= 3.1414/2.0 :
        fac2 = math.cos(wind_dir)*(BF**1.5)
    else:
        fac2 = 0
    
    per = (fac1 + fac2)
    
    return per
