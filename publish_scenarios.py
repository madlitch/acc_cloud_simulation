from google.cloud import pubsub_v1, bigquery
import json

topic_id = 'simulate-scenario'
subscription_id = 'simulate-scenario-sub'
project_id = 'astute-lyceum-418705'
dataset_id = 'highd'
table_id = 'acc_challenging_scenarios'

publisher = pubsub_v1.PublisherClient()
bigquery_client = bigquery.Client(project=project_id)
topic_path = publisher.topic_path(project_id, topic_id)

# load challenging scenarios from BigQuery
query = f"""
SELECT *
FROM `{project_id}.{dataset_id}.{table_id}`
"""
query_job = bigquery_client.query(query)

for row in query_job:
    scenario_data = dict(row)
    message_data = json.dumps(scenario_data).encode("utf-8")
    future = publisher.publish(topic_path, data=message_data)
    print(f"Published scenario {scenario_data['frame']} to {topic_path}: {future.result()}")
