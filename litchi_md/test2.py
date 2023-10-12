import websocket
from time import time
import json
from tools import *

config = DotDict.from_yaml("config.yaml")

if __name__ == '__main__':
    # websocket.enableTrace(True)

    delay_ls = []

    def on_open(ws):
        ws.send(f"{MsgType.register}{RegisterType.receiver}")


    def on_message(ws, msg):

        msg = json.loads(msg)
        # print(msg)
        # print(msg['time'])
        delay = (time() - float(msg['time'])) * 1000
        delay_ls.append(delay)

        print(f"{delay:.4f} ms - avg {sum(delay_ls) / len(delay_ls):.4f} ms")


    ws = websocket.WebSocketApp(
        config.litchi_md_url,
        on_open=on_open,
        on_message=on_message,
    )

    ws.run_forever()
