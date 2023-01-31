import os

import requests
import time
from dotenv import load_dotenv
import sys
load_dotenv()

mg_socket_url = os.environ.get("MG_SOCKET_URL", "")


def send_event(event, event_data, user_id):
    try:
        time1 = time.time()
        response = requests.post(
            mg_socket_url + "/event",
            json={"event": event, "event-data": event_data, "email": user_id},
        )
        
        print("socket_jagad/__init__.py: " + mg_socket_url + "/event" + " " , (time.time() - time1) * 1000,file=sys.stdout,flush=True)
        data = response.json()
        return True

    except Exception as e:
        print(str(e))
        return False
