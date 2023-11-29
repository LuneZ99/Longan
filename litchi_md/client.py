import json
import logging

import websocket

from tools import MsgType, RegisterType, global_config


class LitchiClientSender:

    def __init__(self, name, logger, litchi_url=global_config.litchi_md_url, auto_connect=True):
        super(LitchiClientSender, self).__init__()

        self.name = name
        self.litchi_url = litchi_url
        self.connected = False
        self.auto_connect = auto_connect
        self.client: websocket.WebSocket | None = None
        self.logger = logger
        self.count = 0

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
            self.logger.info(f"Litchi client connected.")
            self.connected = True
        except ConnectionRefusedError:
            self.logger.warning("ConnectionRefusedError, is litchi_md server running?")

    def send(self, msg, retry_count=0):
        if self.auto_connect:
            self.connect()
        if self.client is None:
            return
        try:
            self.client.send(f"{msg}")
            self.count += 1
            if self.count % 10000 == 0:
                self.logger.info(f"Litchi client send {self.count} messages.")
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(msg)
        except ConnectionRefusedError as e:
            self.logger.warning("Litchi client send failed. reconnecting ...")
            self.connected = False
            self.connect()
            if retry_count < 5:
                self.send(msg, retry_count=retry_count+1)
            else:
                self.logger.error(f"Litchi client send failed more than 5 times, is litchi_md server running?")
        except Exception as e:
            self.logger.error(f"Litchi client send error. {e}")

    def broadcast(self, msg: dict):
        msg['sender'] = self.name
        msg_str = json.dumps(msg)
        self.send(f"{MsgType.broadcast}{msg_str}")

    def send_str(self, msg: str):
        self.send(msg)

    def close(self):
        self.client.close()
