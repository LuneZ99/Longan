import websocket
from time import time
import json
from tools import *

config = DotDict.from_yaml("config.yaml")
last_time = time()

if __name__ == '__main__':
    # websocket.enableTrace(True)

    delay_ls = []
    last_time_ls = []


    def on_open(ws):
        ws.send(f"{MsgType.register}{RegisterType.receiver}")


    def on_message(ws, msg):
        global last_time

        msg = json.loads(msg)
        # print(msg)
        # # print(msg['time'])
        delay = (time() * 1000 - float(msg['rec_time']))
        delay_ls.append(delay)
        ls = time() - last_time
        last_time_ls.append(ls)
        last_time = time()
        #
        if len(delay_ls) % 1000 == 0:
            print(f"{delay:.4f} ms - avg {sum(delay_ls[-1000:]) / 1000:.4f} ms -"
                  f" last {last_time_ls[-1] * 1000:.4f} - avg ls {sum(last_time_ls[-1000:]):.4f}")


    ws = websocket.WebSocketApp(
        config.litchi_md_url,
        on_open=on_open,
        on_message=on_message,
    )

    ws.run_forever()
