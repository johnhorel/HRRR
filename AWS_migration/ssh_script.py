# -*- coding: utf-8 -*-
"""
#SSH file transfer file

@Author = Zach Rieck

#Requres AWS.txt & user_info.txt

"""

#Import neccessary packages to force pip installs
import subprocess
import sys
import os

#Run script to install pip
#subprocess.call(['./ get_pip.sh'])
print("Installing pip package...")
subprocess.call("./get_pip.sh", shell=True)

#Define install function to cause pip to install any missing modules
def install(package):
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    except subprocess.CalledProcessError as e:
        print(e)
    
install('paramiko')
install('boto3')
#install('zarr')
#Pyproj is required for pygrib
install('pyproj')
#install('pygrib')
#install('cfgrib')
    
#Import all other modules
#import paramiko
import os.path
#import time
import boto3
import s3fs
import shutil
#import zarr
#import cfgrib
#import pygrib
#import xarray as xr
from os import path
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import HTTPClientError
from boto.s3.connection import S3Connection, Bucket, Key
#from paramiko import SSHClient, BadAuthenticationType
#from sys import stdout

# Delete file if it exists already (debugging purposes)
if path.exists("Staging"):
    shutil.rmtree("Staging")
    
# Declare parameters for SSH connection to kingspeak server
nbytes = 4096
ip = '155.101.26.20'
ip2 = '155.101.26.21'
hostname = 'kingspeak.chpc.utah.edu'
path = '/uufs/chpc.utah.edu/common/home/horel-group7/Pando/hrrr'
port = 22

# Pull in user information from external source
arr = []
with open("user_info.txt", "r+") as f:
    for line in f:
        arr.append(line)
        
val1 = arr[0].split(" = ")
val2 = arr[1].split(" = ")

# Populate to user fields
username = val1[1]
password = val2[1]

# Declare parameters for AWS S3 connectivity
# Pull in keys from external source
arr2 = []
with open("AWS.txt", "r+") as f:
    for line in f:
        arr2.append(line)
        
val1 = arr2[0].split(" = ")
val2 = arr2[1].split(" = ")

# Populate to key fields
ACCESS_KEY = val1[1]
SECRET_KEY = val2[1]

# Access Pando
fs = s3fs.S3FileSystem(anon=True, client_kwargs={'endpoint_url':"https://pando-rgw01.chpc.utah.edu/"})

# List objects in a path and import to array
files = fs.ls('hrrr/sfc/20190101')[-3:]

# Make a staging directory that can hold data as a medium
os.mkdir("Staging")

for file in files:
    item = str(file)
    lst = item.split("/")
    name = lst[3]
    path = "Staging\\" + name
    fs.download(file, path)
    #If file is grib2, convert to zarr
    #if name.endswith('.grib2'):
        #ds_grib = xr.open_dataset(name, engine="cfgrib")
        #print("Converting grib2 files to Zarr...")
        #ds_grib.to_zarr(path + '.zar', consolidated=True)

# Define function that uploads to AWS via Boto3
def upload_to_aws(local_file, bucket, s3_file=None):
    s3 = boto3.client('s3')
                      #, aws_access_key_id = ACCESS_KEY,
                      #aws_secret_access_key = SECRET_KEY)

     # If S3 object_name was not specified, use file_name
    if s3_file is None:
        s3_file = local_file
        
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    # This exception prints if the s3 arguments do not agree with the bucket parameters
    except HTTPClientError as ex:
        print(ex)
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    
# Create function to check for duplicates
def check_duplicates(bucket, key_file, objs):
    if len(objs) > 0 and objs[0].key == key_file:
        s3 = boto3.client('s3')
        s3.delete_object(Bucket = 'transferfrompando', Key = str(key_file))
    else:
        pass
       
# Perform duplicate check
sx = boto3.resource('s3')
bucket = sx.Bucket('transferfrompando')

# Perform file upload to AWS
contents = os.listdir("Staging")
i = 0
print("Uploading files to AWS...")
os.chdir("Staging")
for item in contents:
    # Before uploading, make sure it doesn't already exist in bucket
    key_file = item
    objs = list(bucket.objects.filter(Prefix=key_file))
    duplicate = check_duplicates(bucket, key_file, objs)
    #Upload files
    uploaded = upload_to_aws(key_file, 'transferfrompando')
                             #, 'HRRR_Archive')
    i += 1

# Change dir back to working directory   
os.chdir("..")

# Delete medium directory upon completion
shutil.rmtree("Staging")

    