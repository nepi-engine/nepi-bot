#!/usr/bin/env bash

# shell script to start the nepi_bot software. The following variables
# may be predefined in the environment before running:
#   RUN_LB_LINK=<True|False>   (default is False)
#   RUN_HB_LINK=<True|False>   (default is False)
#   LB_PROC_TIMEOUT=<seconds>  (default is 0 that means no timeout)
#   HB_PROC_TIMEOUT=<seconds>  (default is 0 that means no timeout)

# add current directory to default python path to search for files.

export PYTHONPATH=$PWD:.

python botmain.py
