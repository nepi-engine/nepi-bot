##
## Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.
##
## This file is part of nepi-engine
## (see https://github.com/nepi-engine).
##
## License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
##
docker container run --rm \
	--name nepi-bot -it \
	--mount type=bind,source="$(pwd)"/nepi-bot-if,target=/home/nepi-bot-if \
	nepi-bot-amd64:1.0 