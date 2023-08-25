import asyncio
import websockets


async def handle_message(message):
    print("Received message:", message)
    # 在这里执行您想要的操作


async def send_pong_periodically(websocket):
    while True:
        await asyncio.sleep(600)  # 等待十分钟
        await websocket.ping()



async def subscribe_to_websocket(uri):
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected to", uri)

            # 发送ping帧以保持连接
            await websocket.ping()

            # 启动定时器，定时发送pong帧
            asyncio.create_task(send_pong_periodically(websocket))

            # 进入消息接收循环
            while True:
                message = await websocket.recv()
                await handle_message(message)

    except websockets.ConnectionClosed:
        print("Connection closed for", uri)
        # 处理连接关闭的逻辑，例如尝试重新连接等待一段时间

        # 暂停一段时间后尝试重新连接
        await asyncio.sleep(5)



async def subscribe_to_websockets(uris):
    tasks = []

    for uri in uris:
        tasks.append(asyncio.create_task(subscribe_to_websocket(uri)))

    await asyncio.gather(*tasks)



if __name__ == '__main__':


    # 要订阅的WebSocket链接列表
    uri_lst = [
        "wss://example.com/websocket1",
        "wss://example.com/websocket2",
        "wss://example.com/websocket3"
    ]

    # 运行订阅逻辑
    asyncio.get_event_loop().run_until_complete(subscribe_to_websockets(uri_lst))