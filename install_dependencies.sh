#!/bin/bash

APT_INSTALLS="software-properties-common protobuf-compiler cmake python3-pip"
PIP_INSTALLS="virtualenv"

# Install dependencies
echo "Installing dependencies"
sudo apt install $APT_INSTALLS
pip3 install $PIP_INSTALLS

# Set up virtualenv
echo "Setting up python virtual-env"
cd ./utilities
python3 -m virtualenv venv
source ./venv/bin/activate
cd ..
pip install -r dev-requirements.txt

# Deactivate virtualenv
echo "Deactivating virtualenv... make sure you reactivate before building nepi-bot"
deactivate

echo "nepi-bot dependency installation and setup is complete"

