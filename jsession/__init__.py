import os

import requests
from dotenv import load_dotenv
from requests.structures import CaseInsensitiveDict

load_dotenv()

zep_login_url = os.environ.get("ZEP_LOGIN_URL", "")

zep_pass = os.environ.get("ZEP_PASS", "")
zep_user = os.environ.get("ZEP_USER", "")


def get_jsessionid(user, password):
    try:
        headers = CaseInsensitiveDict()
        headers["Content-Type"] = "application/x-www-form-urlencoded"

        data = f"userName={zep_user}&password={zep_pass}"
        resp = requests.post(zep_login_url, headers=headers, data=data)
        
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
