# Download data daily from https://apps-nefsc.fisheries.noaa.gov/drifter/emolt_QCed.csv

import urllib.request
import pandas as pd


urllib.request.urlretrieve('https://apps-nefsc.fisheries.noaa.gov/drifter/emolt_QCed.csv', 'emolt_QCed.csv')
 
df = pd.read_csv('emolt_QCed.csv')
xr = df.to_xarray()
xr.to_netcdf('emolt_QCed.nc')