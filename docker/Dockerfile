
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
