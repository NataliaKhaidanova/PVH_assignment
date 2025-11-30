# GOAL: Pull events (messages) from Pub/Sub subscription and write the data to the BigQuery warehouse
from google.cloud import pubsub_v1
from google.cloud import bigquery
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

#### FUNSTIONS ---------------------------------------------------------------------------------------------------------
def callback(event) -> None:
    print(f'Received event (message): {event.data}')
    try:
        data = json.loads(event.data.decode('utf-8'))
        print(f"Order ID: {data.get('order_id')}")
        # Transform the data. This is needed mainly because BigQuery expects typed, structured rows and not JSON bytes.
        # We must insure the correct data types since BigQuery expects them. (So, can't make everything TEXT like in
        # PostgreSQL and then transform the data with a database procedure)

        # Can set e.g., event_timestamp to None if we're unable to convert it to isoformat() or source to None if it
        # doesn't exist in the data. But do we want it??? I think it depends on the streaming load and how important
        # these data fields actually are. So, for now kept everything as simple as possible
        row = {
            'order_id': data.get('order_id'),
            'event_type': data.get('event_type'),
            'event_timestamp': parse(data.get('event_timestamp')).isoformat(),
            'order_created_at': parse(data.get('order_created_at')).isoformat(),
            'customer_id': data.get('customer_id'),
            'amount': float(data.get('amount')),
            'currency': data.get('currency'),
            'source': data.get('source'),
            'ingested_at': datetime.utcnow().isoformat() + 'Z'
        }

        errors = bq_client.insert_rows_json(BQ_TABLE, [row]) # Will fail with 403 if you have a free tier
        if errors:
            print(f'BigQuery insert failed: {errors}')
            return # Don't ACK so Pub/Sub will retry the message

        # ACK (remove the message from the Sub/Pub topic)
        event.ack() # Comment out for testing

    except Exception as e:
        print(f'Failed to process event (message): {e}')

def main():
    print(f'Listening for messages on: {subscription_path}')
    stream = subscriber.subscribe(subscription_path, callback=callback)

    # Make the program keep running after you exit the terminal
    try:
        stream.result()
    except KeyboardInterrupt:
        stream.cancel()

# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    main()
