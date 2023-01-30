
import os
from dotenv import load_dotenv
import requests
import datetime as dt

load_dotenv()

mg_mail_notification_url = os.environ.get("MG_MAIL_NOTIFICATION_URL", "")

def send_mail(email, is_success, logs, pipeline_name, user_data, run_date):
    data = {
        "recipient": email,
        "pipelineName": pipeline_name,
        "recipientName": user_data["name"],
        "isSuccess": is_success,
        "logs": logs,
        "runDate": run_date,
    }
    
    # recipient = body["recipient"]
    # recipient_name = body["recipientName"]
    # pipeline_name = body["pipelineName"]
    # run_date = body["runDate"]
    # logs = body["logs"]
    # is_success = body["isSuccess"]
    
    print('send mail data: ', data)
    
    try:
        response = requests.post(mg_mail_notification_url, json=data)
        data = response.json()
        
        print('send mail response: ', data)
    except Exception as e:
        print(str(e))


def handler(data):
    # 0 name
    # 1 enable notification
    # 2 emails
    user = data["user"]
    logs = data["logs"]
    pipeline_name = data["pipelineName"]
    is_success = data["isSuccess"]
    run_date = data["runDate"]
    
    # timestamp float to date string format
    run_date = dt.datetime.fromtimestamp(run_date).strftime("%Y-%m-%d %H:%M:%S")
    
    emails = user["emails"]
    
    for email in emails:
        send_mail(email, is_success, logs, pipeline_name, user, run_date)
