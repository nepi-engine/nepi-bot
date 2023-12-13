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

if [[ -z "${NEPI_SSH_KEY}" ]]; then
  echo "No NEPI_SSH_KEY environment variable... will use the default key path"
  NEPI_SSH_KEY="~/.ssh/numurus/nepi_default_ssh_key_ed25519"
fi

if [[ -z "${NEPI_TARGET_IP}" ]]; then
  echo "No NEPI_TARGET_IP environment variable... will use the default IP"
  NEPI_TARGET_IP="192.168.179.103"
fi

# Also generate the top-level version
git describe --dirty > ./bot_version.txt

rsync -avzhe "ssh -i ${NEPI_SSH_KEY}" --exclude .git --exclude .gitignore --exclude dist --exclude *venv* --exclude build --exclude rsync_src_package_to_target.sh ../nepi-bot/ nepi@${NEPI_TARGET_IP}:/home/nepi/nepi-bot
