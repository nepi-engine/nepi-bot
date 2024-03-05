#!/bin/bash
##
## Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.
##
## This file is part of nepi-engine
## (see https://github.com/nepi-engine).
##
## License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
##


# shell script to start the nepi_bot software. The following variables
# may be predefined in the environment before running:
#   RUN_LB_LINK=<True|False>   (default is False)
#   RUN_HB_LINK=<True|False>   (default is False)
#   LB_PROC_TIMEOUT=<seconds>  (default is 0 that means no timeout)
#   HB_PROC_TIMEOUT=<seconds>  (default is 0 that means no timeout)

# add current directory to default python path to search for files.

display_usage() { 
	echo -e "\nThe following variables may be set in the environment before"
    echo -e "running this script alter nepi-bot behavior:\n"
    echo -e "\tRUN_LB_LINK=<True|False>   (default is True)"
    echo -e "\tRUN_HB_LINK=<True|False>   (default is False)"
    echo -e "\tLB_PROC_TIMEOUT=<seconds>  (default is 0 that means no timeout)"
    echo -e "\tHB_PROC_TIMEOUT=<seconds>  (default is 0 that means no timeout)\n"
	} 

# if "-h" or "--help" supplied, display usage 
	if [[ ( $# -ge 1) || ( "$1" == "--help") ||  ( "$1" == "-h" ) ]] 
	then 
		display_usage
		exit 0
	fi 

export PYTHONPATH=.:$PWD:
export PATH=.:$PATH

python botmain.py
