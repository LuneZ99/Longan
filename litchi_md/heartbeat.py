from litchi_md.client import LitchiClientSender
from tools import get_logger
import time

logger = get_logger("scheduled_task")

heartbeat_client = LitchiClientSender("heartbeat", logger)

while True:
    heartbeat_client.broadcast({
        'event': 'heartbeat',
        'data': {
            'time': time.time(),
        },
        'rec_time': time.time(),
    })
    time.sleep(0.01)

