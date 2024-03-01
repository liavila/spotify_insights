from pymongo import MongoClient
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import json
from datetime import datetime

def load_json(file):
    # Initialize GCS hook
    gcs_hook = GCSHook()

    # Download file content as a string
    file_content = gcs_hook.download(bucket_name=gcs_bucket_processed, object_name=file)

    # Parse the JSON string and return the data
    return json.loads(file_content)

# GCS buckets
gcs_bucket_processed = 'spotify_jsons_for_mongodbatlas'
storage_client = storage.Client()

# Your MongoDB Atlas connection string
mongo_uri = "REPLACE_WITH_URI"
client = MongoClient(mongo_uri)

print(client)
# Format the current date as a string in the format 'YYYY-MM-DD'
today_date_str = datetime.now().strftime('%Y-%m-%d')

# Include the date in the filename
filename_top_tracks = f'processed_top_tracks_data_{today_date_str}.json'
gcs_filename_top_tracks = f'data/{filename_top_tracks}'

# Include the date in the filename
filename_recently_played = f'processed_recently_played_data_{today_date_str}.json'
gcs_filename_recently_played = f'data/{filename_recently_played}'

# Select your database
db = client['Spotify-dds2024']

# Select the collections
collection_recently_played = db['rawdata_recentlyplayed']
collection_top_tracks = db['rawdata_toptracks']

print(db)
print(collection_recently_played)
print(collection_top_tracks)
# Load and upsert the recently played data
recently_played_data = load_json(gcs_filename_recently_played)
for item in recently_played_data:
    filter = {'user_id': item['user_id'],
                'user_name': item['user_name'], 'played_at': item['played_at']}
    collection_recently_played.update_one(
        filter, {'$set': item}, upsert=True)
    
# Load and insert the top tracks data
top_tracks_data = load_json(gcs_filename_top_tracks)
collection_top_tracks.insert_many(top_tracks_data)

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
