#!/bin/ash
# Wait for a little bit before changing run-levels to let the container fully start
sleep 3

# Launch all services with run-level "default"
openrc default

while true; do sleep 1000; done