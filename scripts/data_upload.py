from pymongo import MongoClient
from pymongo.server_api import ServerApi
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import json
from datetime import datetime
import os

# this sets the google credentials for connection
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/airflow/gcs/data/google_credentials.json"


# GCS buckets
gcs_bucket_dag = 'bucket_for_dag'
storage_client = storage.Client()

# Loads the raw data recently played and top tracks from the directory: bucket_for_dag/data/{file_name}


def load_json(file_name):
    # Initialize GCS hook
    gcs_hook = GCSHook()
    destination_path = f"data/{file_name}"

    # Download file content as a string
    file_content = gcs_hook.download(
        bucket_name=gcs_bucket_dag, object_name=destination_path)

    # Parse the JSON string and return the data
    return json.loads(file_content)


# print(client)
# Format the current date as a string in the format 'YYYY-MM-DD'
today_date_str = datetime.now().strftime('%Y-%m-%d')

# Include the date in the filename
filename_top_tracks = f'processed_top_tracks_data_{today_date_str}.json'

# Include the date in the filename
filename_recently_played = f'processed_recently_played_data_{today_date_str}.json'

# Load and insert the top tracks data
top_tracks_data = load_json(filename_top_tracks)

# Load and insert the top tracks data
recently_played_data = load_json(filename_recently_played)


# Uploading the json data imported from the bucket to MongoDB Atlas
# Define it in the environment variables
mongo_uri = os.getenv("MONGO_URI")

client = MongoClient(mongo_uri, server_api=ServerApi('1'))

# Select your database
db = client['Spotify-dds2024']

# To see if there is a connection
server_info = client.server_info()
print('Pymongo Version:', server_info['version'])

# Select the collections
collection_recently_played = db['rawdata_recentlyplayed']
collection_top_tracks = db['rawdata_toptracks']

# Insert the data to collections
# collection_recently_played.insert_many(recently_played_data)
# collection_top_tracks.insert_many(top_tracks_data)

# Upsert the recently played data
for item in recently_played_data:
    update_criteria = {
        'user_id': item['user_id'], 'user_name': item['user_name'], 'played_at': item['played_at']}
    collection_recently_played.update_one(
        update_criteria, {'$set': item}, upsert=True)

# Upsert the top tracks data
for item in top_tracks_data:
    update_criteria = {
        'user_id': item['user_id'], 'user_name': item['user_name']}
    collection_top_tracks.update_one(
        update_criteria, {'$set': item}, upsert=True)


# For local run:
# # Load and upsert the recently played data
# with open(filename_recently_played, 'r') as file:
#     recently_played_data = json.load(file)
#     collection_recently_played = db['rawdata_recentlyplayed']
#     for item in recently_played_data:
#         filter = {'user_id': item['user_id'],
#                   'user_name': item['user_name'], 'played_at': item['played_at']}
#         collection_recently_played.update_one(
#             filter, {'$set': item}, upsert=True)

# # Load and insert the top tracks data
# with open(filename_top_tracks, 'r') as file:
#     top_tracks_data = json.load(file)
# collection_top_tracks.insert_many(top_tracks_data)

print("Data uploaded successfully to the respective collections in the projectspotify database.")

client.close()  # to ensure resources are released
