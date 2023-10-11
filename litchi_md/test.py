import json

import websocket
from time import sleep, time

from tools import *

config = DotDict.from_yaml("config.yaml")

# websocket.enableTrace(True)

# print(config.litchi_md_url)
# litchi_md = websocket.create_connection("ws://localhost:8011")


def on_open(ws):
    ws.send("sender")


# litchi_md = websocket.WebSocketApp(
#     config.litchi_md_url,
#     on_open=on_open,
# )

litchi_md = websocket.create_connection(config.litchi_md_url)


for i in range(5):
    litchi_md.send(json.dumps(
        {
            MsgKey.type: MsgType.register,
            MsgKey.data: RegisterType.sender
        }
    ))
    sleep(0.1)

for i in range(1000):
    litchi_md.send(json.dumps(
        {
            MsgKey.type: MsgType.market_data,
            MsgKey.data: {
                "time": time(),
                "b": 0
            }
        }
    ))
    sleep(0.001)

litchi_md.close()


