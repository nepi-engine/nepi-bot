docker container run --rm \
	--name nepi-bot -it \
	--mount type=bind,source="$(pwd)"/nepi-bot-if,target=/home/nepi-bot-if \
	nepi-bot-amd64:1.0 