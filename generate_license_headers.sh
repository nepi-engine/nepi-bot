#!/bin/bash

# Leverage licenseheaders Python module to update license headers
# Requires licenseheaders is on python3 path (e.g., pip3 install licenseheaders) 
# and the NEPI source header license text template file is passed in as first argument
# to this script

TEMPLATE_FILE=$1
TARGET_DIR='.'
EXCLUSIONS='*generate_license_headers.sh* *nepi_edge_sw_mgr* *nepi-protobuf*'
OWNER='Numurus, LLC <https://www.numurus.com>'
PROJECT='nepi-bot'
PROJECT_URL='https://github.com/numurus-nepi/nepi-bot'

# Create a dummy Python file to generate the LICENSE file
touch ${TARGET_DIR}/license_dummy.py

# Generate headers
python3 -m licenseheaders -t ${TEMPLATE_FILE} -d ${TARGET_DIR} -x ${EXCLUSIONS} -o "${OWNER}" -n ${PROJECT} -u ${PROJECT_URL} -cy

# Remove leading '#' to generate the LICENSE file
sed -i -e 's/^#//g' ${TARGET_DIR}/license_dummy.py
mv ${TARGET_DIR}/license_dummy.py LICENSE
