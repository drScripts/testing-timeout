
import os
from dotenv import load_dotenv
import requests

load_dotenv()

mg_mail_notification_url = os.environ.get("MG_MAIL_NOTIFICATION_URL", "")

def handler(data):
    
    pass