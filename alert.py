import time
from elasticsearch import Elasticsearch
import requests
import json
import urllib3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from datetime import datetime, timedelta
# from elasticsearch.helpers import StreamRequest
# es = Elasticsearch(['https://perf.taleemabad.com:9200'])
http = urllib3.PoolManager()

teams_webhook_url = 'TEAMWEBHOOKURL'

es = Elasticsearch(
 ['https://elkhost:9200'],
  http_auth=('username', 'pass'), 
  verify_certs=False,         # Set to True if your using a trusted SSL certificate

)

index_name = "kibana-alert"

def send_alert(document_data):
 


    file_path = "data.json"
    data_dict = json.loads(document_data)
    try:
        with open(file_path, "r") as file:
            existing_data = json.load(file)
    except FileNotFoundError:
        existing_data = []

    # Function to find the index of an item in the list based on a key
    def find_index_by_rule_id(data_list, rule_id):
        for index, item in enumerate(data_list):
            if item["rule_id"] == rule_id:
                return index
        return -1

    # Check if the new data has the same rule_id as an existing record
    index = find_index_by_rule_id(existing_data, data_dict["rule_id"])
    data_dict['date'] = datetime.strptime(data_dict['date'], "%Y-%m-%dT%H:%M:%S.%fZ")
    data_dict['date'] = data_dict['date'] + timedelta(hours=5)
    data_dict['date']=data_dict['date'].strftime("%Y-%m-%d %H:%M:%S")
    message = {
    "@context": "https://schema.org/extensions",

    "@type": "MessageCard",
    "themeColor":"#0000ff",
    "title": "Kibana-Alert",
     "text": f"**Alert ID:** {data_dict['alert_id']}\n\n"
            f"**Rule ID:** {data_dict['rule_id']}\n\n"
            f"**Reason:** {data_dict['reason']}\n\n"
            f"**Service Name:** {data_dict['service_name']}\n\n"
            f"**Date:** {data_dict['date']}"
    # "text": document_data
    } 
    if index != -1:
        # If the rule_id exists, compare the dates
        if data_dict["date"] > existing_data[index]["date"]:
            # Replace the old record with the new data
            existing_data[index] = data_dict
            response = http.request('POST',teams_webhook_url, headers={'Content-Type': 'application/json'}, body=str(message).encode('utf-8'))
    else:
        # If the rule_id does not exist, append the new data to the list
        existing_data.append(data_dict)
        response = http.request('POST',teams_webhook_url, headers={'Content-Type': 'application/json'}, body=str(message).encode('utf-8'))

    # Write the updated data back to the file
    with open(file_path, "w") as file:
        json.dump(existing_data, file, indent=2)

    # Send the message to the Teams channel
    # try: 

    #     response = http.request('POST',teams_webhook_url, headers={'Content-Type': 'application/json'}, body=str(message).encode('utf-8'))
    # except Exception as e:
    #     print("error is", str(e))
    # print("response",response.data)


def watch_elasticsearch_index():
    query = {
    "size": 0,
    "aggs": {
        "group_by_alert_rule": {
            "terms": {
                "field": "rule_id"
            },
            "aggs": {
                "latest_rule": {
                    "top_hits": {
                        "size": 1,
                        "sort": [
                            {
                                "date": {
                                    "order": "desc"
                                }
                            }
                          ]
                         }
                     }
                  }
                }
             }
        }
    results = es.search(index=index_name, body=query)
    try:
        for bucket in results.get("aggregations", {}).get("group_by_alert_rule", {}).get("buckets", []):
            latest_alert = bucket.get("latest_rule", {}).get("hits", {}).get("hits", [])[0]
            alert_data = latest_alert.get("_source", {})
            send_alert(json.dumps(alert_data, indent=2))
    except Exception as e:
        print("hi",str(e))



if __name__ == "__main__":
    watch_elasticsearch_index()




