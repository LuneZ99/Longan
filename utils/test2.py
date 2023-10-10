from utils import config
import websocket
from time import time

if __name__ == '__main__':
    websocket.enableTrace(True)


    def on_message(ws, message):
        print((time() - float(message)) * 1000, "ms")

    ws = websocket.WebSocketApp(
        "ws://localhost:8010",
        on_message=on_message,
    )

    ws.run_forever()

