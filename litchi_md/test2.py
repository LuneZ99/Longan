import websocket
from time import time
import json
from tools import *

config = DotDict.from_yaml("config.yaml")

if __name__ == '__main__':
    # websocket.enableTrace(True)

    def on_open(ws):
        ws.send(json.dumps(
            {
                MsgKey.type: MsgType.register,
                MsgKey.data: RegisterType.receiver
            }
        ))


    def on_message(ws, msg):
        msg = json.loads(msg)

        print((time() - float(msg[MsgKey.data]['time'])) * 1000, "ms")


    ws = websocket.WebSocketApp(
        config.litchi_md_url,
        on_open=on_open,
        on_message=on_message,
    )

    ws.run_forever()
