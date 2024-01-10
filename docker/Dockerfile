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

ARG ARCH
FROM ${ARCH}/alpine:latest

# Provide a startup system manager via openrc
RUN apk add --no-cache openrc

# Provide an SSH server via openssh
RUN apk add --no-cache openssh

# Ensure SSH server is enabled on start-up
RUN rc-update add sshd

# Copy in the CUI-compliant config files for SSH services
COPY ./resources/sshd_config /etc/ssh/sshd_config

# Create the unpriviledged user
RUN adduser --disabled-password nepi-user
RUN echo nepi-user:nepi-user | chpasswd
USER nepi-user
RUN mkdir /home/nepi-user/.ssh
COPY ./resources/id_ecdsa.pub /home/nepi-user/.ssh/authorized_keys

# Add a start-up script
USER root
COPY ./resources/nepi-bot-docker-entrypoint.sh /usr/local/bin
RUN chmod +x /usr/local/bin/nepi-bot-docker-entrypoint.sh

ENTRYPOINT ["nepi-bot-docker-entrypoint.sh"]
