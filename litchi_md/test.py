from time import sleep, time
import json
import socket
import websocket

from tools import *

config = DotDict.from_yaml("config.yaml")


import websocket

litchi_md = websocket.create_connection(config.litchi_md_url)

for i in range(5):
    litchi_md.send(f"{MsgType.register}{RegisterType.sender}")
    sleep(0.1)

for i in range(10000):

    if not litchi_md.connected:
        print('The server is closed. Stopping the message sending.')
        break

    dic = {
        "time": time(),
        "b": 0
    }

    try:
        litchi_md.send(f"{MsgType.broadcast}{json.dumps(dic)}")
    except ConnectionError:
        print('The server is closed. Stopping the message sending.')
        break

    # print(i, litchi_md.connected)

    sleep(0.0001)

litchi_md.close()
