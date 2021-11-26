# Download emolt Lobster Boat data daily from 'http://emolt.org/emoltdata/emolt_QCed.csv'
# Script and data being run and stored on RI-PRO-03_VM01 (10.90.209.10)
# csv is downloaded from emoltURL and a timestamp added to the csv, placed in downloadDirectory.
# csv converted to a temp nc file, enhanced to meet CF and ACCD standards.
# Enhanced nc output to two places:
#   1. Archived to ncOutputArchiveDirectory with timestamp on filename.
#   2. Most current/Lastest/Newest enhanced nc file output to ncLatestEnhancedDirectory
# Crontab to be setup on RI-PRO-03_VM01 (10.90.209.10) tio run this script daily.

import os, sys
import pathlib
import urllib
import pandas as pd
import numpy as np
import xarray as xr
import yaml
from datetime import datetime
import __future__

emoltURL = 'http://emolt.org/emoltdata/emolt_QCed.csv'
now = datetime.now()
# downloadFileName = 'emolt_' + now.strftime("%d%b%Y_%H%M") + '.csv'
ncLatestEnhancedFileName = 'emolt_enhanced.nc'
ncOutputArchiveFileName = 'emolt_enhanced_' + now.strftime("%Y%m%d%H%M") + '.nc'

### Local Directories for testing
downloadDirectory = './'
downloadFileName = 'emolt_' + now.strftime("%d%b%Y_%H%M") + '.csv'
ncOutputArchiveDirectory = './nc' + now.strftime("%Y%m%d%H%M") + '/'
ncLatestEnhancedDirectory = './'


### VM Directories
# downloadDirectory = '/data/maracoos/emolt/download/'
# ncOutputArchiveDirectory = '/data/maracoos/emolt/nc/' + now.strftime("%d%b%Y_%H%M") + '/'
# ncLatestEnhancedDirectory = '/data/maracoos/emolt/'

# Give correct permissions to output directory in Linux. 
if not pathlib.Path(ncOutputArchiveDirectory).exists():
    os.makedirs(ncOutputArchiveDirectory, mode=0o776) # drwxrwxrw-

def create_date_metadata_modified(ds):
    """
    Create the global metadata field 'date_metadata_modified' and populate
    with the current timestamp.

    :param xarray.Dataset object: dataset to modify
    """

    ds.attrs['date_metadata_modified'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

def override_variable_attrs_from_template(ds, meta):
    """Change the units in a given dataset to those in a dict of metadata.
    Args:
        ds(xarray.Dataset): open xarray dataset
        meta(dict): dict of metadata info to add
    Returns:
        None"""

    # iterate through the keys of the dict
    for k, vdict in meta.items():
        # if key in the dataset as a variable, assign
        if (k in ds):
             ds[k].attrs.update(vdict)
# csv = urllib.request.urlretrieve(emoltURL, downloadDirectory+downloadFileName)
try:
    csv = urllib.request.urlretrieve(emoltURL, downloadDirectory+'emolt_QCed.csv')
    # csv = urllib2.urlopen('http://emolt.org/emoltdata/emolt_QCed.csv')
except:
    print ('Failed to download CSV file.')
    # sys.exit(1)
print (csv)
df = pd.read_csv(csv[0])
df_to_xr  = df.to_xarray()
df_to_xr.to_netcdf('emolt_QCed.nc')
ds = xr.open_dataset('emolt_QCed.nc') 
print (ds)
# Drop the variables in place.
ds = ds.drop('Unnamed: 0')
ds = ds.drop('vessel')

# Rename Variables
ds = ds.rename({
    'lat': 'latitude',
    'lon': 'longitude',
    'datet': 'time',
    'mean_temp': 'temperature',
    'std_temp': 'stdtemperature'
})

# Set Metadata for file and variables
metadata_yml = 'emolt_metadata.yml'
variables_yml = 'emolt_variables.yml'
with open(metadata_yml) as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    metadata_dict = yaml.load(file, Loader=yaml.FullLoader)

with open(variables_yml) as file2:
    variables_dict = yaml.load(file2, Loader=yaml.FullLoader)

# Update global and variable metadata with dicts
ds.attrs.update(metadata_dict)
override_variable_attrs_from_template(ds, variables_dict)

# Add Edit Metadata Timestamp
create_date_metadata_modified(ds)

# Parse date_created from an existing global metadata field history
ds.attrs["date_created"] = ds.attrs['date_metadata_modified']

# 2.2 Data Types
# * The variable index failed because the datatype is int64
# * The variable flag failed because the datatype is int64
ds['flag'] = ds.flag.astype('int32')
ds['index'] = ds.index.astype('int32')

# Convert time to Unix Epoch Time
_to_timestamp = lambda x: ((x - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's'))
times = np.apply_along_axis(_to_timestamp, 0, ds['time'].values.astype('<M8[s]'))
times
ds['time'].values = times

# Remove  _FillValues enconding to make writeable to netCDF.
for v in ds:
    try:
        del ds[v].encoding["_FillValue"]
    except:
        continue

# Delete any previously created Current Enhanced Output file
if os.path.exists(ncLatestEnhancedDirectory+ncLatestEnhancedFileName):
  os.remove(ncLatestEnhancedDirectory+ncLatestEnhancedFileName)

# Write updated enhanced output .nc files to latest and archive locations.
ds.to_netcdf(ncLatestEnhancedDirectory+ncLatestEnhancedFileName, format = "NETCDF3_64BIT")
ds.to_netcdf(ncOutputArchiveDirectory+ncOutputArchiveFileName, format = "NETCDF3_64BIT")

# Delete temp .nc file
if os.path.exists("temp.nc"):
  os.remove("temp.nc")
    