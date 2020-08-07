#!/usr/bin/env python

import argparse
import json
from datetime import datetime
import threading, queue

def pubsub2bq(
    project_id, subscription_id, dataset_id, table_id, timeout=None
):
    """Receives messages from a pull subscription with flow control."""
    # [START pubsub_subscriber_flow_settings]
    from google.cloud import pubsub_v1
    from google.cloud import bigquery

    # queue
    q = queue.Queue()

    # TODO(developer)
    # project_id = "your-project-id"
    # subscription_id = "your-subscription-id"
    # Number of seconds the subscriber should listen for messages
    # timeout = 5.0

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project_id, subscription_id
    )

    def callback(message):
        print("Received message: {}".format(message.data))
        jsonmsg = json.loads(message.data)
        jsonmsg['timestamp'] = datetime.utcfromtimestamp(jsonmsg['timestamp']).isoformat()
        print('json: {}'.format(jsonmsg))
        q.put(jsonmsg)
        message.ack()

    # bigquery client
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    def worker_bq():
        print('Running worker_bq')
        t = threading.currentThread()
        while getattr(t, 'do_run', True):
            jsonmsg = q.get()
            print('bq load')
            job = client.load_table_from_json([jsonmsg], table_ref)
            job.result()
        print('Finish worker_bq')


    # Limit the subscriber to only have ten outstanding messages at a time.
    flow_control = pubsub_v1.types.FlowControl(max_messages=10)

    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=callback, flow_control=flow_control
    )
    print("Listening for messages on {}..".format(subscription_path))
    print("Insert messages to {}..\n".format(table_ref.path))

    # start worker_bq thread
    t = threading.Thread(target=worker_bq, name='worker_bq')
    t.start()
    
    print('Start subscriber')
    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()
    setattr(t, 'do_run', False)
    t.join()
    # [END pubsub_subscriber_flow_settings]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("project_id", help="Google Cloud project ID")
    parser.add_argument("subscription_id", help="PubSub subscription ID")
    parser.add_argument("dataset_id", help="BigQuery dataset ID")
    parser.add_argument("table_id", help="BigQuery table ID")

    args = parser.parse_args()
    pubsub2bq(args.project_id, args.subscription_id, args.dataset_id, args.table_id)
