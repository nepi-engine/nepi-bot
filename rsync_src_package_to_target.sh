#!/bin/bash
##
## NEPI Dual-Use License
## Project: nepi-bot
##
## This license applies to any user of NEPI Engine software
##
## Copyright (C) 2023 Numurus, LLC <https://www.numurus.com>
## see https://github.com/numurus-nepi/nepi-bot
##
## This software is dual-licensed under the terms of either a NEPI software developer license
## or a NEPI software commercial license.
##
## The terms of both the NEPI software developer and commercial licenses
## can be found at: www.numurus.com/licensing-nepi-engine
##
## Redistributions in source code must retain this top-level comment block.
## Plagiarizing this software to sidestep the license obligations is illegal.
##
## Contact Information:
## ====================
## - https://www.numurus.com/licensing-nepi-engine
## - mailto:nepi@numurus.com
##
##

#######################################################################################################
# Usage: $ ./rsync_workspace_to_target
#
# This script is intended for the scenario where NEPI source code is cloned (and modified as applicable
# on a development host and then pushed to the NEPI target device to be built natively on that device.
#
# If you are cloning NEPI source directly to the target device, you do not need to run this script.
#
# The script leverages some environment variables if they exist to allow user customization:
#    NEPI_SSH_KEY: Path to SSH private keyfile for the NEPI device
#    NEPI_TARGET_IP: IP address (or resolvable hostname) for the NEPI device
# In the absence of these environment variables the script attempts to use hard-coded defaults.
#######################################################################################################

if [[ -z "${NEPI_SSH_KEY}" ]]; then
  echo "No NEPI_SSH_KEY environment variable... will use the default key path"
  NEPI_SSH_KEY="~/.ssh/numurus/nepi_default_ssh_key_ed25519"
fi

if [[ -z "${NEPI_TARGET_IP}" ]]; then
  echo "No NEPI_TARGET_IP environment variable... will use the default IP"
  NEPI_TARGET_IP="192.168.179.103"
fi

if [[ -z "${NEPI_TARGET_SRC_DIR}" ]]; then
  NEPI_TARGET_SRC_DIR="/mnt/nepi_storage/nepi_src"
  echo "No NEPI_TARGET_SRC_DIR environment variable... will use default ${NEPI_TARGET_SRC_DIR}"
fi

# Also generate the top-level version
git describe --dirty > ./bot_version.txt

# Avoid pushing local build artifacts, git stuff, and any local venv modules that are locally installed
RSYNC_EXCLUDES="--exclude rsync_src_package_to_target.sh --exclude .git* \
--exclude dist --exclude *venv* --exclude build"

# Push (almost) everything to the specified source folder on the target
rsync -avzhe "ssh -i ${NEPI_SSH_KEY} -o StrictHostKeyChecking=no" ${RSYNC_EXCLUDES} ../nepi-bot/ nepi@${NEPI_TARGET_IP}:${NEPI_TARGET_SRC_DIR}/nepi-bot
