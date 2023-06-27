#!/bin/bash

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
