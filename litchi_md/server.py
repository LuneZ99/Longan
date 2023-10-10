import asyncio
import websockets

from tools import DotDict
import logging


config = DotDict.from_yaml("config.yaml")


logger_litchi_md = logging.getLogger('logger_litchi_md')
logger_litchi_md.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler("litchi_md.log")
file_handler.setFormatter(formatter)
logger_litchi_md.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger_litchi_md.addHandler(console_handler)


connected = set()


async def broadcast_handle(websocket, path):
    # Register.
    connected.add(websocket)

    try:
        async for message in websocket:
            # Broadcast to all connected clients.
            await asyncio.gather(*[ws.send(message) for ws in connected if ws != websocket])
    finally:
        # Unregister.
        connected.remove(websocket)


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
