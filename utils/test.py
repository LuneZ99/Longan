from utils import config
import websocket
from time import sleep, time

websocket.enableTrace(True)

# print(config.litchi_md_url)

# litchi_md = websocket.create_connection(config.litchi_md_url)
litchi_md = websocket.create_connection("ws://localhost:8010")


for i in range(300):
    litchi_md.send(str(time()))
    sleep(0.0001)

litchi_md.close()


