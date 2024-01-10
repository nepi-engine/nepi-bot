#
# NEPI Dual-Use License
# Project: nepi-bot
#
# This license applies to any user of NEPI Engine software
#
# Copyright (C) 2023 Numurus, LLC <https://www.numurus.com>
# see https://github.com/numurus-nepi/nepi-bot
#
# This software is dual-licensed under the terms of either a NEPI software developer license
# or a NEPI software commercial license.
#
# The terms of both the NEPI software developer and commercial licenses
# can be found at: www.numurus.com/licensing-nepi-engine
#
# Redistributions in source code must retain this top-level comment block.
# Plagiarizing this software to sidestep the license obligations is illegal.
#
# Contact Information:
# ====================
# - https://www.numurus.com/licensing-nepi-engine
# - mailto:nepi@numurus.com
#
#
import socket

host = "10.0.0.116"
port = 7770
bsiz = 1024
addr = (host, port)


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(addr)
s.listen(5)

idone = False
while True:
    if idone:
        break
    print("Waiting for Connection:")
    ns, naddr = s.accept()
    print '...connected from:', naddr
    while True:
        data = ns.recv(bsiz)
        if not data or data == "quit":
            print "Received 'quit' Message."  
            idone = True
            ns.shutdown(socket.SHUT_RDWR)
            ns.close()
            break
        print "Received: " + data.encode("hex")  

s.close()
