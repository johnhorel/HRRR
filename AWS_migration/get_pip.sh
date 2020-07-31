#!bin/bash
# Shell script to obtain pip for python modules

# Pull in pip from site
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py

# Install
python get-pip.py

