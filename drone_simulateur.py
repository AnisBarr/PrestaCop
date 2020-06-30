#!/usr/bin/env python3

import socket
import random
from time import sleep
import datetime

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 9999        # Port to listen on (non-privileged ports are > 1023)
id_iot = "38fda9b7-a9da-4663-9403-f2e7c658eea0"
id_sensor = ["1e81286f-ad9f-489e-ae13-4faa9f8069bd", "bd00228e-5641-4341-922d-545eeb9bbfaa", "f403590d-068e-49cf-9613-1c28e9c5810d"]
temp = 24.5

lat = 40
log = -73

temp_aberante = 150
false_positive_time = 100
temp_error = "ERROR"
error_time = 120
compteur_abe = 0
compteur_error = 0



with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    with conn:
        while True:
            line = f"{id_iot};{temp_aberante if compteur_abe % false_positive_time == 0 else temp_error if compteur_error % error_time == 0 else temp};{datetime.datetime.now().timestamp()};{random.choice(id_sensor)}"
            conn.sendall(line.encode('UTF-8'))
            sleep(1)
            compteur_abe = compteur_abe + 1
            compteur_error = compteur_error + 1
            temp = temp + random.choice([-0.1, 0, 0.1])
