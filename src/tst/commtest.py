import os
import sys
import time
import serial


print("Start Program")
print("Establish Communcations Connection.")
print("Set Serial Port.")
sys.stdout.flush()
serialport = serial.Serial()
#serialport.port = "/dev/ttyUSB0"
serialport.port = "COM5"
serialport.baudrate = 19200
serialport.timeout = 3

print("Open Serial Port")
sys.stdout.flush()
if not serialport.isOpen():
    serialport.open()
    print("Opened.")
else:
    print("Serial Port Already Opened.")
sys.stdout.flush()

command = [b'AT+CGMI', b'AT+CGMM', b'AT+CGMR', b'AT+CGSN']


def acquire_response(command, sp, wait_time):
    try:
        sp.reset_input_buffer()
        sp.write(command + b'\r')
    except Exception as e:
        print("FAILED: [" + str(e) + "]")
        if sp.isOpen():
            sp.close()

    message = ""
    timeout = time.time() + wait_time

    while time.time() < timeout:
        if sp.in_waiting == 0:
            time.sleep(1)
            continue

        try:
            message += sp.readline()
            print("Message: [" + str(message) + "]")
            
            if "OK" in message:
                if command[2:] in message:
                    start_idx = message.index('\n') + 1
                    end_idx = message.index("OK")
                    message = message[start_idx:end_idx]
                    message = message.strip()
                    return message

                return True
                    
            elif "READY" in message:
                return True

        except Exception as e:
            print("FAILED: [" + str(e) + "]")
            if sp.isOpen():
                sp.close()
            break

    print("FAILED: [Timeout]")
    sys.stdout.flush()
    return False


isu_name = acquire_response(command[0], serialport, 10)
print("isu_name: [" + str(isu_name) + "]")
sys.stdout.flush()

isu_model_number = acquire_response(command[1], serialport, 10)
print("isu_model_number: [" + str(isu_model_number) + "]")
sys.stdout.flush()

isu_version = acquire_response(command[2], serialport, 10)
print("isu_version: [" + str(isu_version) + "]")
sys.stdout.flush()

isu_model_number = acquire_response(command[3], serialport, 10)
print("isu_model_number: [" + str(isu_model_number) + "]")
sys.stdout.flush()

sys.exit(0)

