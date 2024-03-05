#!/bin/bash
##
## Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.
##
## This file is part of nepi-engine
## (see https://github.com/nepi-engine).
##
## License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
##

if [ "$#" -ne 1 ]; then
	echo "Usage: build.sh <arch>"
	echo "   where <arch> is one of the supported Alpine Docker architectures:"
	echo "   amd64, arm32v6, arm32v7, arm64v8, i386, ppc641e, s390x"
	exit 1
fi

docker image build -t nepi-bot-$1:1.0 --build-arg ARCH=$1 .