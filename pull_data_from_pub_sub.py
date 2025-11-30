# GOAL: Pull events (messages) from Pub/Sub subscription and write the data to the BigQuery warehouse
from google.cloud import pubsub_v1, bigquery
from dateutil.parser import parse
from datetime import datetime
import json

#### CONSTANTS ---------------------------------------------------------------------------------------------------------
# In real life (company scenario) must be stored securely
PROJECT_ID = 'pvh-assignment'
SUBSCRIPTION_ID = 'orders-events-topic-sub'
BQ_TABLE = 'pvh-assignment.orders.order_events_streaming'

#### INIT --------------------------------------------------------------------------------------------------------------
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
bq_client = bigquery.Client(project=PROJECT_ID)

#### HELPER FUNCTIONS --------------------------------------------------------------------------------------------------
# I've read that in streaming pipelines, we want to avoid "poison messages". This is different from SQL ETLs where we
# set as strict rules as possible because the pipelines have restricted run times, and we can afford fixing every single
# error. So, if a timestamp or numeric value is malformed, we set them to None instead of crashing and retrying forever
def convert_to_timestamp(value):
    try:
        return parse(value).isoformat()
    except Exception:
        return None

def convert_to_float(value):
    try:
        return float(value)
    except Exception:
        return None

#### FUNCTIONS ---------------------------------------------------------------------------------------------------------
def callback(event) -> None:
    print(f'Received event (message): {event.data}')
    try:
        data = json.loads(event.data.decode('utf-8'))
        print(f"Order ID: {data.get('order_id')}")

        # Transform the data. This is needed mainly because BigQuery expects typed, structured rows and not JSON bytes.
        # We must ensure the correct data types since BigQuery expects them. (So, can't make everything TEXT like in
        # PostgreSQL and then transform the data with a database procedure)
        row = {
            'order_id': data.get('order_id'),
            'event_type': data.get('event_type'),
            'event_timestamp': convert_to_timestamp(data.get('event_timestamp')),
            'order_created_at': convert_to_timestamp(data.get('order_created_at')),
            'customer_id': data.get('customer_id'),
            'amount': convert_to_float(data.get('amount')),
            'currency': data.get('currency'),
            'source': data.get('source'),
            'ingested_at': datetime.utcnow().isoformat() + 'Z'
        }

        # Insert the row into BigQuery (Will fail with 403 if you have a free tier)
        errors = bq_client.insert_rows_json(BQ_TABLE, [row])
        if errors:
            print(f'BigQuery insert failed: {errors}')
            return  # Don't ACK so Pub/Sub will retry the message

        # ACK (remove the message from the Sub/Pub topic)
        event.ack()  # Comment out for testing

    except Exception as e:
        print(f'Failed to process event (message): {e}')

def main():
    print(f'Listening for messages on: {subscription_path}')
    stream = subscriber.subscribe(subscription_path, callback=callback)

    try:
        stream.result()
    except KeyboardInterrupt:
        stream.cancel() # Make the program keep running after you exit the terminal

# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    main()
