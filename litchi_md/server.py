import asyncio
import websockets

from tools import DotDict
import logging


config = DotDict.from_yaml("config.yaml")


logger_litchi_md = logging.getLogger('logger_litchi_md')
logger_litchi_md.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s | %(message)s')

file_handler = logging.FileHandler("litchi_md.log")
file_handler.setFormatter(formatter)
logger_litchi_md.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger_litchi_md.addHandler(console_handler)


senders = set()
receivers = set()


async def broadcast_handle(websocket, path):
    # First message should be the type of the client.
    client_type = await websocket.recv()
    if client_type == 'sender':
        senders.add(websocket)
    elif client_type == 'receiver':
        receivers.add(websocket)
    else:
        print(f"Unknown client type: {client_type}")
        return

    try:
        async for message in websocket:
            # If the message is from a sender, broadcast it to all receivers.
            if websocket in senders:
                await asyncio.wait([ws.send(message) for ws in receivers])
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
