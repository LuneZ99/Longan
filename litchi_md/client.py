import json

import websocket

from tools import *


class LitchiClientSender:

    def __init__(self, name, litchi_url="ws://localhost:8010", logger_folder=None, auto_connect=True):
        super(LitchiClientSender, self).__init__()

        self.name = name
        self.litchi_url = litchi_url
        self.connected = False
        self.auto_connect = auto_connect
        self.client: websocket.WebSocket | None = None
        self.logger = get_logger(
            f"litchi_client_{name}",
            logger_dir=logger_folder or f"{logger_folder}/log.litchi.{name}"
        )

        if auto_connect:
            self.connect()

    def __del__(self):
        self.close()

    def connect(self):
        if self.connected:
            return
        try:
            self.client = websocket.create_connection(self.litchi_url)
            self.client.send(f"{MsgType.register}{RegisterType.sender}")
            self.logger.info(f"litchi_client connected")
        except ConnectionRefusedError:
            self.logger.warning("ConnectionRefusedError, is litchi_md server running?")

    def send(self, msg: dict):
        if self.auto_connect:
            self.connect()
        if self.client is None:
            return
        msg['sender'] = self.name
        msg_str = json.dumps(msg)
        self.client.send(f"{MsgType.broadcast}{msg_str}")
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(msg_str)

    def close(self):
        self.client.close()
