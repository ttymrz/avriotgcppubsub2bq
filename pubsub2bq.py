#!/usr/bin/env python

import argparse
import json
from datetime import datetime

def pubsub2bq(
    project_id, subscription_name, dataset_id, table_id, timeout=None
):
    """Receives messages from a pull subscription with flow control."""
    # [START pubsub_subscriber_flow_settings]
    from google.cloud import pubsub_v1
    from google.cloud import bigquery

    # TODO project_id = "Your Google Cloud Project ID"
    # TODO subscription_name = "Your Pub/Sub subscription name"
    # TODO timeout = 5.0  # "How long the subscriber should listen for
    # messages in seconds"

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project_id, subscription_name
    )

    # bigquery client
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    def callback(message):
        print("Received message: {}".format(message.data))
        jsonmsg = json.loads(message.data)
        jsonmsg['timestamp'] = datetime.utcfromtimestamp(jsonmsg['timestamp']).isoformat()
        print('json: {}'.format(jsonmsg))
        job = client.load_table_from_json([jsonmsg], table_ref)
        job.result()
        message.ack()

    # Limit the subscriber to only have ten outstanding messages at a time.
    flow_control = pubsub_v1.types.FlowControl(max_messages=10)

    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=callback, flow_control=flow_control
    )
    print("Listening for messages on {}..".format(subscription_path))
    print("Insert messages to {}..\n".format(table_ref.path))

    # result() in a future will block indefinitely if `timeout` is not set,
    # unless an exception is encountered first.
    try:
        streaming_pull_future.result(timeout=timeout)
    except:  # noqa
        streaming_pull_future.cancel()
    # [END pubsub_subscriber_flow_settings]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("project_id", help="Google Cloud project ID")
    parser.add_argument("subscription_name", help="PubSub subscription name")
    parser.add_argument("dataset_id", help="BigQuery dataset ID")
    parser.add_argument("table_id", help="BigQuery table ID")

    args = parser.parse_args()
    pubsub2bq(args.project_id, args.subscription_name, args.dataset_id, args.table_id)
