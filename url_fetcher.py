import sys
import requests

def get_token():
    email = "system@system.com"
    password = "123123"
    
    try:
        login_response = requests.post(
            "https://database-query.v3.microgen.id/api/v1/fb6db565-2e6c-41eb-bf0f-66f43b2b75ae/auth/login",
            json={"email": email, "password": password},
        )
        
        data = login_response.json()
        
        if login_response.status_code == 200:
            return data["token"]
        else:
            print("Failed to get system token")
            return None
    except requests.exceptions.RequestException as e:
        print(str(e))
        return None


def hello(mg_pipeline_id):
    print("mg_pipeline_id:", mg_pipeline_id)
    
    token = get_token()
    print('token: ', token)
    if token is not None:
        try:
            data = {"mgPipelineId": mg_pipeline_id, "cron": "true"}

            try:
                response = requests.post(
                    "https://dontdqcopl.function.microgen.id/pipeline-run", json=data, headers={"Authorization": "Bearer " + token}
                )
                
                status_code = response.status_code
                print('status_code: ', status_code)

                data = response.json()
                print("data: ", data)

            except requests.exceptions.RequestException as e:
                print(str(e))

        except Exception as e:
            print(str(e))
    else: 
        print("Failed to get token, Unauthorized")


if __name__ == "__main__":
    mg_pipeline_id = sys.argv[1]
    hello(mg_pipeline_id)
