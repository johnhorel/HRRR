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

# Access Pando
fs = s3fs.S3FileSystem(anon=True, client_kwargs={'endpoint_url':"https://pando-rgw01.chpc.utah.edu/"})
# The ls command can be slow, so we can force the read_timeout to be patient
fs.read_timeout = 100000000000000

# Create a staging directory to hold data temporarily
os.mkdir("Staging")

# Need to create file structure similar to Pando and copy data files into it
# Start with sfc files
# Let x = 1st day data was collected
# All data is archived back to the same date
x = 20160715 
# Compare to day program is run to prevent missing any days
today = datetime.today().strftime('%Y%m%d')
t = int(today)
print("Staging files...")
os.mkdir("Staging/sfc")
while x < t:
    # Make sfc dir
    os.mkdir("Staging/sfc/" + str(x))
    # Make prs dir
    os.mkdir("Staging/prs" + str(x))
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
    # Account for 00 year non-leap yrs (not divisible by 400)
    if yr.endswith('00') and int(yr) % 400 != 0:
        x +=72
    # February leap years
    if str(x).endswith('0229') and int(yr) % 4 == 0:
        x += 71
    else:
        x += 1
    try:
         # Run one at a time
         files = fs.ls('hrrr/sfc/' + str(x))[-3:]
         #files2 = fs.ls('hrrr/prs/' + str(x))[-3:]
    # Catch in case counter tries to pull non-existant day
    except FileNotFoundError:
        continue
    # Import sfc files
    for file in files:
    #for file in files2:
        item = str(file)
        lst = item.split("/")
        idx = len(lst) - 1
        name = lst[idx]
        path = "Staging/sfc/" + dest + "/" + name
        #path = "Staging/prs/" + dest + "/" + name
        fs.download(file, path)

# Upload files from local dir using rclone
print("Uploading to AWS...")
base = os.getcwd()
result = rclone.with_config(cfg).run_cmd(command="sync", extra_args=[base + "/Staging/sfc", "AWS test:transferfrompando/sfc"]) 
#result = rclone.with_config(cfg).run_cmd(command="sync", extra_args=[base + "/Staging/prs", "AWS test:transferfrompando/prs"]) 

# Change dir back to working directory   
os.chdir("..")

# Delete medium directory upon completion
shutil.rmtree("Staging")

    
