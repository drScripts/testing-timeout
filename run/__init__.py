import json
import os
import time
import uuid
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from pytz import timezone

from jsession import get_jsessionid
from socket_jagad import send_event
from solr import store_to_solr

load_dotenv()

pipeline_api_url = os.environ.get("PIPELINE_API_URL", "")
zeppelin_api_url = os.environ.get("ZEP_API_URL", "")
notebook_log_url = os.environ.get("NOTEBOOK_LOG_URL", "")

mg_email = os.environ.get("MG_EMAIL", "")
mg_password = os.environ.get("MG_PASSWORD", "")
mg_login_url = os.environ.get("MG_LOGIN_URL", "")

mg_pipeline_url = os.environ.get("MG_PIPELINE_URL", "")
mg_pipelog_url = os.environ.get("MG_PIPELOG_URL", "")
mg_zep_user_url = os.environ.get("MG_ZEP_USER_URL", "")


def run_pipeline(note_id, pipeId, process_order_index, flow_length, jsonify, z_user, z_pass):
    # check if job["notebook"]["id"] is not empty
    if not note_id or note_id == "":
        return (
            False,
            {
                "pipeline_id": pipeId,
                "flow_sequence": process_order_index + 1,
                "flow_length": flow_length,
                "notebook_id": note_id,
                "paragraph_logs": [],
                "is_success": 0,
                "message": "Notebook id is empty",
                "elapsed_time": 0,
            },
        )

    jsessionid = get_jsessionid(z_user, z_pass)

    if jsessionid == "ErrorJsessionid":
        return jsonify({"error": "unable get jsessionid"}), 401

    zep_job_url = zeppelin_api_url + "/job/" + note_id

    t = time.process_time()

    headers = {
        "Cookie": jsessionid,
    }

    try:
        response = requests.get(
            zep_job_url,
            headers=headers,  # type: ignore
        )

        data = json.dumps(response.json())
        data = json.loads(data)

        if response.status_code != 200:
            return (
                False,
                {
                    "pipeline_id": pipeId,
                    "flow_sequence": process_order_index + 1,
                    "flow_length": flow_length,
                    "notebook_id": note_id,
                    "is_success": 0,
                    "message": data["message"]
                    if "message" in data
                    else "Error get notebook detail",
                    "elapsed_time": 0,
                },
            )

        paragraphs = data["body"]

        paragraph_logs = []
        paragraph_error = False

        for i, paragraph in enumerate(paragraphs):
            # zeppelin paragraph log url
            zep_log_url = (
                zeppelin_api_url + "/" + note_id +
                "/paragraph/" + paragraph["id"]
            )

            # get zeppelin paragraph log
            try:
                t = time.process_time()
                response_log = requests.get(
                    zep_log_url,
                    headers=headers,  # type: ignore
                )

                data_log = json.dumps(response_log.json())
                data_log_loads = json.loads(data_log)

                results = data_log_loads["body"]["results"]

                if results["code"] == "SUCCESS":
                    paragraph_logs.append(
                        {
                            "is_success": 1 if results["code"] == "SUCCESS" else 0,
                            "messages": results["msg"],
                            "text": data_log_loads["body"]["text"],
                            "elapsed_time": time.process_time() - t,
                        }
                    )
                elif results["code"] == "ERROR":
                    paragraph_logs.append(
                        {
                            "is_success": 1 if results["code"] == "SUCCESS" else 0,
                            "messages": results["msg"],
                            "text": data_log_loads["body"]["text"],
                            "elapsed_time": time.process_time() - t,
                        }
                    )
                    paragraph_error = True
                    break
                else:
                    print("No results")
            except Exception as e:
                print(str(e))
                return (
                    False,
                    {
                        "pipeline_id": pipeId,
                        "flow_sequence": process_order_index + 1,
                        "flow_length": flow_length,
                        "notebook_id": note_id,
                        "is_success": 0,
                        "message": str(e),
                        "elapsed_time": time.process_time() - t,
                    },
                )

        elapsed_time = time.process_time() - t

        solr_data = {
            "pipeline_id": pipeId,
            "flow_sequence": process_order_index + 1,
            "flow_length": flow_length,
            "notebook_id": note_id,
            "is_success": 0 if response.status_code != 200 or paragraph_error else 1,
            "paragraph_logs": paragraph_logs,
            "elapsed_time": elapsed_time,
        }

        # if status is not 200
        if response.status_code != 200 or paragraph_error:
            return (False, solr_data)

        return (True, solr_data)
    except Exception as e:
        print(str(e))
        return (
            False,
            {
                "pipeline_id": pipeId,
                "flow_sequence": process_order_index + 1,
                "flow_length": flow_length,
                "notebook_id": note_id,
                "is_success": 0,
                "message": str(e),
                "elapsed_time": time.process_time() - t,
            },
        )


def get_pipelines(id, jsonify):
    finished_flow = []
    has_error = False

    try:
        response = requests.get(
            pipeline_api_url + "/" + id, headers={"Authorization": ""}
        )
        json_res = response.json()

        flow_job = json.loads(json_res["flow_job"])
        # print('flow_job: ', flow_job)

        if not isinstance(flow_job, list):
            return print("flowJob is not array")

        for process_order_index, jobs in enumerate(flow_job):
            # print('jobs: ', jobs)

            if not isinstance(jobs, list):
                return print("job is not array")

            for job in jobs:
                try:
                    note_id = job["notebook"]["id"]
                    zep_user = job["zepUser"]
                    zep_pass = job["zepPass"]
                except Exception as e:
                    print(str(e))
                    return print("notebook id, zepUser, zepPass is required")
                if not note_id == "" or not zep_user == "" or not zep_pass == "":
                    return print("notebook id, zepUser, zepPass is required")
                # run notebook
                run = run_pipeline(
                    note_id,
                    id,
                    process_order_index,
                    len(flow_job),
                    jsonify,
                    zep_user,
                    zep_pass
                )
                finish = run[1]
                finish["label"] = job["data"]["label"]

                finished_flow.append(finish)

                if run[0] == False:
                    has_error = True
                    break
                else:
                    pass
            else:
                continue
            break

            # print('jobs1: ', jobs)

        res = {
            "total": len(flow_job)
            if len(flow_job) - len(finished_flow) >= 0
            else len(finished_flow),
            "finished": len(finished_flow),
            "unfinished": 0
            if len(flow_job) - len(finished_flow) < 0
            else len(flow_job) - len(finished_flow),
            "logs": finished_flow,
            "has_error": has_error,
        }
        return res
    except Exception as e:
        print(str(e))
        return str(e)


def handler(request, jsonify):
    t = time.process_time()
    ts = time.time()

    # Get the request body
    body = request.get_json()
    headers = request.headers

    try:
        bearer_token = headers["Authorization"]
    except:
        return jsonify({"message": "Unauthorized"}), 401

    try:
        cron = True if body["cron"] == "true" else False
    except:
        cron = False

    try:
        mg_pipeline_id = body["mgPipelineId"]
        token = ""

        if len(mg_pipeline_id) == 0:
            return jsonify({"message": "mgPipelineId is required"}), 422
        else:
            # try:
            # response = requests.post(
            #     mg_login_url, json={
            #         "email": mg_email, "password": mg_password}
            # )

            # data = json.dumps(response.json())
            # token = json.loads(data)["token"]

            # check mg pipeline
            try:
                response_pipe = requests.get(
                    mg_pipeline_url + "/" + mg_pipeline_id,
                    headers={"Authorization": bearer_token},
                )

                d = json.dumps(response_pipe.json())
                # throw error if status code is not 200
                if response_pipe.status_code != 200:
                    return jsonify({"message": d}), 422

            except Exception as e:
                return jsonify({"message": str(e)}), 500

            # except Exception as e:
            #     print(str(e))

    except:
        return jsonify({"message": "mgPipelineId is required"}), 422

    res = get_pipelines(mg_pipeline_id, jsonify)
    res = json.loads(json.dumps(res))

    status_code = 200 if res["has_error"] == False else 400

    elapsed_time = time.process_time() - t

    result = {
        "message": "Finished",
        "result": res,
    }

    # login to mg with email and password
    try:
        url = mg_pipeline_url + "/" + mg_pipeline_id
        # print("url: ", url)
        # now = datetime.now()
        # utc = timezone("UTC")
        # utc_time = now.astimezone(utc)

        # get mg pipeline
        response = requests.get(url)
        data = json.dumps(response.json())

        createdBy = json.loads(data)["createdBy"]
        notification = json.loads(data)["notification"]

        print("notification:", notification)
        print("createdBy:", createdBy)

        # patch mg pipeline
        try:
            status = "SUCCESS" if status_code == 200 else "ERROR"

            response = requests.patch(
                url,
                json={
                    "last_run": str(time.time()),
                    "last_status": status,
                },
                headers={"Authorization": "Bearer " + token},
            )

            if cron == True and notification == True and createdBy != None:
                # post pipelog
                try:
                    post_data = {
                        "timestamp": str(time.time()),
                        "status": status,
                        "pipeline": [mg_pipeline_id],
                        "user": [createdBy],
                    }

                    response = requests.post(
                        mg_pipelog_url,
                        json=post_data,
                        headers={"Authorization": "Bearer " + token},
                    )

                    # print("post pipelog: ", response.json())

                    if response.status_code != 200 and response.status_code != 201:
                        send_event(
                            event="pipelog",
                            event_data={
                                "message": "error post pipelog: "
                                + str(response.json()),
                                "status": status,
                                "mg-pipeline-id": mg_pipeline_id,
                            },
                            user_id=createdBy,
                        )

                    else:
                        send_event(
                            event="pipelog",
                            event_data={
                                "status": status,
                                "mg-pipeline-id": mg_pipeline_id,
                            },
                            user_id=createdBy,
                        )

                except Exception as e:
                    print(str(e))

            elif cron == True and notification != True and createdBy != None:
                send_event(
                    event="pipelog",
                    event_data={
                        "status": status,
                        "mg-pipeline-id": mg_pipeline_id,
                    },
                    user_id=createdBy,
                )

        except Exception as e:
            print(str(e))

    except Exception as e:
        print(str(e))

    solr_data = json.dumps(res, indent=4, sort_keys=True, default=str)

    store_to_solr(solr_data, mg_pipeline_id, ts, elapsed_time)

    return jsonify(result), status_code
