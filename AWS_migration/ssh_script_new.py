# -*- coding: utf-8 -*-
"""
SSH file transfer file

@Author = Zach Rieck

Requres AWS.txt & user_info.txt

Requires Python v3
"""

#Import neccessary packages to force pip installs
import subprocess
import sys
import os

#Run script to install pip
print("Installing pip package...")
subprocess.call("./get_pip.sh", shell=True)

# Perform rclone installation and build config file
# Also loads rclone module
print("Installing rclone...")
subprocess.call("./rclone_auto.sh", shell=True)

#Define install function to cause pip to install any missing modules
def install(package):
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    except subprocess.CalledProcessError as e:
        print(e)

# Load python-rclone config
install('python-rclone')
import rclone
h = os.environ.get('HOME')
cfg_path = h + r'/.config/rclone/rclone.conf'

with open(cfg_path) as f:
   cfg = f.read()

# Other installs
install('paramiko')
install('boto3')
install('pyproj')    

#Import all other modules
import os.path
import boto3
import s3fs
import shutil
import requests
from requests.adapters import TimeoutSauce
from requests import exceptions
from os import path
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import HTTPClientError
from datetime import datetime

# Delete file if it exists already
if path.exists("Staging"):
    shutil.rmtree("Staging")
    
# Declare parameters for SSH connection to kingspeak server
nbytes = 4096
ip = '155.101.26.20'
ip2 = '155.101.26.21'
hostname = 'kingspeak.chpc.utah.edu'
path = '/uufs/chpc.utah.edu/common/home/horel-group7/Pando/hrrr'
port = 22

'''
No longer needed (keep code in case cached credentials are needed later)
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
'''

# Access Pando
fs = s3fs.S3FileSystem(anon=True, client_kwargs={'endpoint_url':"https://pando-rgw01.chpc.utah.edu/"})
# The ls command can be slow, so we can force the read_timeout to be patient
fs.read_timeout = 100000000000000

# Create a staging directory to hold data temporarily
os.mkdir("Staging")

# Need to create file structure similar to Pando and copy data files into it
# Start with sfc files
# Let x = 1st day data was collected
x = 20160715 
# Compare to day program is run to prevent missing any days
today = datetime.today().strftime('%Y%m%d')
t = int(today)
print("Staging files...")
os.mkdir("Staging/sfc")
while x < t:
    os.mkdir("Staging/sfc/" + str(x))
    yr = str(x)[0:4]
    dest = str(x)
    # Start new year
    if str(x).endswith('1231'):

        x += 8869
    # 31 day months
    if str(x).endswith('31'):
        x += 69
    # 30 day months
    if str(x).endswith('0430') or str(x).endswith('0630') or str(x).endswith('0930') or str(x).endswith('1130'):
        x += 70
    # February non-leap years
    if str(x).endswith('0228') and int(yr) % 4 != 0:
        x += 72
    # February leap years
    if str(x).endswith('0229') and int(yr) % 4 == 0:
        x += 71
    else:
        x += 1
    try:
         files = fs.ls('hrrr/sfc/' + str(x))[-3:]
    # Catch in case counter tries to pull non-existant day
    except FileNotFoundError:
        continue
    # Import files
    for file in files:
        item = str(file)
        lst = item.split("/")
        idx = len(lst) - 1
        name = lst[idx]
        path = "Staging/sfc/" + dest + "/" + name
        fs.download(file, path)

# Upload files from local dir using rclone
print("Uploading to AWS...")
base = os.getcwd()
result = rclone.with_config(cfg).run_cmd(command="sync", extra_args=["/base/Staging/sfc", "AWS test:transferfrompando/sfc"])

# Define function that uploads to AWS via Boto3
'''
Delete in next release
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
'''

# Change dir back to working directory   
os.chdir("..")

# Delete medium directory upon completion
shutil.rmtree("Staging")

    
