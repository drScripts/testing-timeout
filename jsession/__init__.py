import os

import requests
import time
from dotenv import load_dotenv
from requests.structures import CaseInsensitiveDict
import sys

load_dotenv()

zep_login_url = os.environ.get("ZEP_LOGIN_URL", "")

# zep_pass = os.environ.get("ZEP_PASS", "")
# zep_user = os.environ.get("ZEP_USER", "")


def get_jsessionid(zep_user, zep_pass):
    try:
        
        print(zep_user,zep_pass)
        headers = CaseInsensitiveDict()
        headers["Content-Type"] = "application/x-www-form-urlencoded"

        data = f"userName={zep_user}&password={zep_pass}"
        time1 = time.time()
        resp = requests.post(zep_login_url, headers=headers, data=data)
        print("jsession/__init__.py: " + zep_login_url + " " , (time.time() - time1) * 1000,file=sys.stdout)
        if resp.status_code != 200:
            return "ErrorJsessionid"

        # response cookies
        res_cookies=resp.cookies
        jsession_id=res_cookies['JSESSIONID']
        print('jsession_id: ', jsession_id)
        
        return "JSESSIONID=" + jsession_id
    except:
      print('An exception occurred')
      return "ErrorJsessionid"
