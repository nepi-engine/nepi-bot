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
