import json
import re
from datetime import datetime
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pathlib import Path

# PATH = Path("/Users/irerielunicornio/Documents/USF/Spring1/Distributed-Data-Systems/Final Project/scripts")

# For local run:
# # Load the JSON data
# with open(PATH/'top_tracks_data.json', 'r', encoding='utf-8') as file:
#     data = json.load(file)

# Set the bucket name
gcs_bucket_dag = 'bucket_for_dag'
storage_client = storage.Client()


def load_json():
    # Initialize GCS hook
    gcs_hook = GCSHook()

    # Download file content as a string
    file_content = gcs_hook.download(
        bucket_name=gcs_bucket_dag, object_name="data/top_tracks_data.json")

    # Parse the JSON string and return the data
    return json.loads(file_content)


def create_and_upload_json(bucket_name, destination_blob_name, data_contents):
    """Creates a file and uploads it to the specified GCS bucket."""
    json_file = json.dumps(data_contents, indent=4)
    destination_path = f"""data/{destination_blob_name}"""

    # Get the bucket object
    bucket = storage_client.bucket(bucket_name)

    # Create a new blob and upload the file's content
    blob = bucket.blob(destination_path)
    blob.upload_from_string(json_file)
    print(f"File uploaded to {bucket_name}/{destination_path}.")


# Get current date
current_date = datetime.now().strftime('%Y-%m-%d')


# Function to clean song names
def clean_song_name(song_name):
    # Use regex to find the first part of the song name before any special characters
    match = re.match(r"^[^\(\[\{]*", song_name)
    return match.group(0).strip() if match else song_name


# Load the data from the bucket where data_fetcher.py uploads
data = load_json()
# Process the data
processed_data = []
for user in data:
    user_id = user['user_id']
    user_name = user['name']
    track_counter = 1
    for item in user['top_tracks']['items']:
        artist_names_concatenated = ', '.join(
            artist['name'] for artist in item['artists'])
        clean_name = clean_song_name(item['name'])
        track_info = {
            'user_id': user_id,
            'user_name': user_name,
            'song_id': item['id'],
            'artist_ids': [artist['id'] for artist in item['artists']],
            'top_track_song_name': item['name'],
            'rank': track_counter,
            'artist_names': ', '.join(artist['name'] for artist in item['artists']),
            'song_link': item['external_urls']['spotify'],
            'artist_song_name': f"{artist_names_concatenated} - {clean_name}",
            'danceability': item['audio_features']['danceability'],
            'energy': item['audio_features']['energy'],
            'key': item['audio_features']['key'],
            'loudness': item['audio_features']['loudness'],
            'mode': item['audio_features']['mode'],
            'speechiness': item['audio_features']['speechiness'],
            'acousticness': item['audio_features']['acousticness'],
            'instrumentalness': item['audio_features']['instrumentalness'],
            'liveness': item['audio_features']['liveness'],
            'valence': item['audio_features']['valence'],
            'tempo': item['audio_features']['tempo'],
            'time_signature': item['audio_features']['time_signature'],
            'date_data_fetched': current_date,
            'images': [{'url': image['url'], 'height': image['height'], 'width': image['width']}
                       for image in item['album']['images']]
        }
        processed_data.append(track_info)
        track_counter += 1


# Format the current date as a string in the format 'YYYY-MM-DD'
today_date_str = datetime.now().strftime('%Y-%m-%d')

# Include the date in the filename
filename = f'processed_top_tracks_data_{today_date_str}.json'

# # Save the processed data to a new JSON file
# with open(PATH/filename, 'w', encoding='utf-8') as outfile:
#     json.dump(processed_data, outfile, indent=4)

create_and_upload_json(gcs_bucket_dag, filename, processed_data)

print(f"Processed data saved to {gcs_bucket_dag}/data/{filename}")
