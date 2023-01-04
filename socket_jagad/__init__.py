import os

import requests
from dotenv import load_dotenv

load_dotenv()

mg_socket_url = os.environ.get("MG_SOCKET_URL", "")


def send_event(event, event_data, user_id):
    try:
        response = requests.post(
            mg_socket_url + "/event",
            json={"event": event, "event-data": event_data, "email": user_id},
        )
        data = response.json()
        print(data)
        return True

    except Exception as e:
        print(str(e))
        return False
