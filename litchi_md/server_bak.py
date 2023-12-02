import asyncio
import websockets
import time

from tools import *

# TODO change to dict to support subscriptions
senders = set()
receivers = set()
count = 0


async def broadcast_handle(websocket: websockets.WebSocketServerProtocol):
    try:
        async for msg in websocket:
            msg_type = msg[0]
            if msg_type == MsgType.broadcast:
                global count
                count += 1
                if count % 10000 == 0:
                    print(f"broadcast count {count}")
                # If the message is from a sender, broadcast it to all receivers.
                if len(receivers) > 0:
                    data = msg[1:]
                    tasks = [ws.send(data) for ws in receivers if ws != websocket]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for ws, result in zip(receivers, results):
                        if isinstance(result, Exception):
                            receivers.remove(ws)
            elif msg_type == MsgType.register:
                data = msg[1:]
                if data == RegisterType.sender:
                    senders.add(websocket)
                    print(f"add sender {data}")
                elif data == RegisterType.receiver:
                    receivers.add(websocket)
                    print(f"add receiver {data}")
                else:
                    pass
            else:
                pass
    finally:
        if websocket in senders:
            senders.remove(websocket)
            print(f"del sender")
        if websocket in receivers:
            receivers.remove(websocket)
            print(f"del receiver")


md_server = websockets.serve(
    broadcast_handle,
    # config.litchi_md_ip,
    # config.litchi_md_port,
    # logger=logger_litchi_md,
    "localhost",
    8010,
    ping_interval=None,
    ping_timeout=None,
    close_timeout=None,
    max_size=2 ** 20, max_queue=2 ** 5, read_limit=2 ** 16, write_limit=2 ** 16
)

asyncio.get_event_loop().run_until_complete(md_server)
asyncio.get_event_loop().run_forever()

