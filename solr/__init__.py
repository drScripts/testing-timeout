import os
import time

import pysolr
import requests

solr_url = os.environ.get("SOLR_URL", "")

def store_to_solr(data, mg_pipeline_id, ts, elapsed_time):
    try:
        solr = pysolr.Solr(solr_url, always_commit=True)

        # add a document
        response = solr.add(
            [
                {
                    "pipeline_id": mg_pipeline_id,
                    "timestamp": ts,
                    "elapsed_time": elapsed_time,
                    "datas": data,
                }
            ]
        )
        print("response: ", response)

        return True
    except Exception as e:
        print(str(e))
        return False
