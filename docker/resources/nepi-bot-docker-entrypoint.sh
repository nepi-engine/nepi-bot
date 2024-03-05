#!/bin/ash
##
## Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.
##
## This file is part of nepi-engine
## (see https://github.com/nepi-engine).
##
## License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
##

# Wait for a little bit before changing run-levels to let the container fully start
sleep 3

# Launch all services with run-level "default"
openrc default

while true; do sleep 1000; done