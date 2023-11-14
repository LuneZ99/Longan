import json
import logging

import websocket

from tools import get_logger, MsgType, RegisterType


class LitchiClientSender:

    def __init__(self, name, logger, litchi_url="ws://localhost:8010", auto_connect=True):
        super(LitchiClientSender, self).__init__()

        self.name = name
        self.litchi_url = litchi_url
        self.connected = False
        self.auto_connect = auto_connect
        self.client: websocket.WebSocket | None = None
        self.logger = logger

        if auto_connect:
            self.connect()

    def __del__(self):
        self.close()

    def connect(self):
        if self.connected:
            return
        try:
            self.client = websocket.create_connection(self.litchi_url)
            self.client.send_str(f"{MsgType.register}{RegisterType.sender}")
            self.logger.info(f"litchi_client connected")
        except ConnectionRefusedError:
            self.logger.warning("ConnectionRefusedError, is litchi_md server running?")

    def broadcast(self, msg: dict):
        if self.auto_connect:
            self.connect()
        if self.client is None:
            return
        msg['sender'] = self.name
        msg_str = json.dumps(msg)
        self.client.send(f"{MsgType.broadcast}{msg_str}")
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(msg_str)

    def send_str(self, msg: str):
        if self.auto_connect:
            self.connect()
        if self.client is None:
            return
        self.client.send(msg)
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(msg)

    def close(self):
        self.client.close()
