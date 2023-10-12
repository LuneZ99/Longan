import asyncio
import websockets

from tools import *
import logging
import json


config = DotDict.from_yaml("config.yaml")


if config.litchi_md_log:
    logger_litchi_md = logging.getLogger('logger_litchi_md')
    logger_litchi_md.setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s | %(message)s')

    file_handler = logging.FileHandler("litchi_md.log")
    file_handler.setFormatter(formatter)
    logger_litchi_md.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger_litchi_md.addHandler(console_handler)
else:
    logger_litchi_md = None

# TODO change to dict to support subscriptions
senders = set()
receivers = set()


async def broadcast_handle(websocket, path):

    # First message should be the type of the client.
    # msg = await websocket.recv()
    # if msg == 'sender':
    #     senders.add(websocket)
    # elif msg == 'receiver':
    #     receivers.add(websocket)
    # else:
    #     print(f"Unknown client type: {msg}")
    #     return

    try:
        async for msg in websocket:

            msg_type = msg[0]

            if msg_type == MsgType.register:
                data = msg[1:]
                if data == RegisterType.sender:
                    senders.add(websocket)
                elif data == RegisterType.receiver:
                    receivers.add(websocket)
                else:
                    if logger_litchi_md:
                        logger_litchi_md.warning(f"Unknown client type: {msg}")

            # If the message is from a sender, broadcast it to all receivers.
            if websocket in senders and msg_type == MsgType.broadcast and len(receivers) > 0:
                send_msg = msg[1:]
                await asyncio.gather(*[ws.send(send_msg) for ws in receivers])

    finally:
        # Unregister.
        if websocket in senders:
            senders.remove(websocket)
        if websocket in receivers:
            receivers.remove(websocket)


md_server = websockets.serve(
    broadcast_handle,
    config.litchi_md_ip,
    config.litchi_md_port,
    logger=logger_litchi_md,
    ping_interval=None,
    ping_timeout=None,
    close_timeout=None
)

asyncio.get_event_loop().run_until_complete(md_server)
asyncio.get_event_loop().run_forever()
