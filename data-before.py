from faker import Faker
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.table import TimePartitioningType
from google.oauth2 import service_account
import datetime
import random

# Instantiate Faker
fake = Faker()

# Define number of records
n_records = 10000000

# Define systems
systems = ['System_A', 'System_B', 'System_C', 'System_D', 'System_E']

# Define events
events = ['Event1', 'Event2', 'Event3', 'Event4', 'Event5']

# Generate data
data = {
    "datetime": [fake.date_time_this_decade() for _ in range(n_records)],
    "event": [random.choice(events) for _ in range(n_records)],
    "system": [random.choice(systems) for _ in range(n_records)],
    "duration": [random.uniform(0.1, 60.0) for _ in range(n_records)],
}

# Convert to DataFrame
df = pd.DataFrame(data)

client = bigquery.Client()

# Define BigQuery dataset and table
dataset_id = 'events'
table_id = 'log'

# Create a BigQuery table
table_ref = client.dataset(dataset_id).table(table_id)
table = bigquery.Table(table_ref)

# Load DataFrame into BigQuery
job = client.load_table_from_dataframe(df, table)
job.result()  # Wait for the job to complete

print(f'Loaded {job.output_rows} rows into {dataset_id}:{table_id}.')