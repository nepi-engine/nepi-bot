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

if [ "$#" -ne 1 ]; then
	echo "Usage: build.sh <arch>"
	echo "   where <arch> is one of the supported Alpine Docker architectures:"
	echo "   amd64, arm32v6, arm32v7, arm64v8, i386, ppc641e, s390x"
	exit 1
fi

docker image build -t nepi-bot-$1:1.0 --build-arg ARCH=$1 .