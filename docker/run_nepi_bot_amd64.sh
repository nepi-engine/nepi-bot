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
docker container run --rm \
	--name nepi-bot -it \
	--mount type=bind,source="$(pwd)"/nepi-bot-if,target=/home/nepi-bot-if \
	nepi-bot-amd64:1.0 