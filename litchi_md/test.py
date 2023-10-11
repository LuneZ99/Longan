import websocket
from time import sleep, time

from tools.dot_dict import DotDict

config = DotDict.from_yaml("config.yaml")

websocket.enableTrace(True)

# print(config.litchi_md_url)
# litchi_md = websocket.create_connection("ws://localhost:8011")


def on_open(ws):
    ws.send("sender")


litchi_md = websocket.WebSocketApp(
    config.litchi_md_url,
    on_open=on_open,
)


for i in range(5):
    litchi_md.send(str(time()))
    sleep(0.1)

litchi_md.close()


