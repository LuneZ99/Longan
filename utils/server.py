import asyncio
import websockets

connected = set()


async def broadcast_handle(websocket, path):
    # Register.
    connected.add(websocket)
    print(websocket.request_headers)

    try:
        async for message in websocket:
            # Broadcast to all connected clients.
            await asyncio.gather(*[ws.send(message) for ws in connected if ws != websocket])
    finally:
        # Unregister.
        connected.remove(websocket)


start_server = websockets.serve(
    broadcast_handle,
    "localhost", 8010,
    ping_interval=None, ping_timeout=None, close_timeout=None
)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
