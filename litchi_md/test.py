import websocket
from time import sleep, time

from tools.dot_dict import DotDict

config = DotDict.from_yaml("config.yaml")

websocket.enableTrace(True)

# print(config.litchi_md_url)

litchi_md = websocket.create_connection(config.litchi_md_url)


for i in range(60):
    litchi_md.send(str(time()))
    sleep(0.1)

litchi_md.close()


