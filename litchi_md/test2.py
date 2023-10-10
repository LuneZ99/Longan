import websocket
from time import time

from tools.dot_dict import DotDict

config = DotDict.from_yaml("config.yaml")


if __name__ == '__main__':
    websocket.enableTrace(True)


    def on_message(ws, message):
        print((time() - float(message)) * 1000, "ms")

    ws = websocket.WebSocketApp(
        config.litchi_md_url,
        on_message=on_message,
    )

    ws.run_forever()

