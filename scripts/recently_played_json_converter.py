import json
from datetime import datetime, timedelta
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from dotenv import load_dotenv
from pathlib import Path
import re
import pytz

# For local run:
# PATH = Path("/Users/irerielunicornio/Documents/USF/Spring1/Distributed-Data-Systems/Final Project/scripts")

# # Load the JSON data
# with open(PATH/'recently_played_data.json', 'r', encoding='utf-8') as f:
#     data = json.load(f)

# Define it in the environment variables
gcs_bucket_dag = 'bucket_for_dag'

storage_client = storage.Client()


def load_json():
    # Initialize GCS hook
    gcs_hook = GCSHook()

    # Download file content as a string
    file_content = gcs_hook.download(
        bucket_name=gcs_bucket_dag, object_name="data/recently_played_data.json")

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


def clean_song_name(song_name):
    # Use regex to find the first part of the song name before any special characters
    match = re.match(r"^[^\(\[\{]*", song_name)
    return match.group(0).strip() if match else song_name


# Load the data from GCS bucket
data = load_json()

# Process the data
processed_data = []
for user in data:  # Loop through each user
    user_id = user['user_id']
    user_name = user['name']
    for item in user['recently_played']['items']:
        images_info = [{'url': image['url'], 'height': image['height'],
                        'width': image['width']} for image in item['track']['album']['images']]

        played_at = datetime.fromisoformat(
            item['played_at'][:-1]).astimezone(pytz.timezone('America/Los_Angeles'))
        eight_hours_earlier = played_at - timedelta(hours=8)
        formatted_timestamp = eight_hours_earlier.strftime(
            '%Y-%m-%d %H:%M:%S PT')

        artist_ids = [artist['id'] for artist in item['track']['artists']]
        artist_spotify_urls = [artist['href']
                               for artist in item['track']['artists']]

        clean_name = clean_song_name(item['track']['name'])
        artist_names_concatenated = ', '.join(
            artist['name'] for artist in item['track']['artists'])
        track_info = {
            'user_id': user_id,
            'user_name': user_name,
            'track_id': item['track']['id'],
            'artists_id': artist_ids,
            'album_id': item['track']['album']['id'],
            'artist_names': artist_names_concatenated,
            'track_name': item['track']['name'],
            'artist_song_name': f"{artist_names_concatenated} - {clean_name}",
            'album_name': item['track']['album']['name'],
            'album_type': item['track']['album']['album_type'],
            'duration': '{:02d}:{:02d}'.format(*divmod(int(item['track']['duration_ms']) // 1000, 60)),
            'played_at': formatted_timestamp,
            'popularity': item['track']['popularity'],
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
            'preview_url': item['track']['preview_url'],
            'track_spotify_url': item['track']['external_urls']['spotify'],
            'artists_spotify_url': artist_spotify_urls,
            'release_date': item['track']['album']['release_date'],
            'is_local': item['track']['is_local'],
            'explicit': item['track']['explicit'],
            'total_tracks': item['track']['album']['total_tracks'],
            'type': item['track']['type'],
            'context_type': item['context'].get('type', 'none') if item.get('context') else 'none',
            'images': images_info
        }
        processed_data.append(track_info)


# Format the current date as a string in the format 'YYYY-MM-DD'
today_date_str = datetime.now().strftime('%Y-%m-%d')

# Include the date in the filename
filename = f'processed_recently_played_data_{today_date_str}.json'

# For local run:
# # Save the processed data to a new JSON file
# with open(PATH/filename, 'w', encoding='utf-8') as f:
#     json.dump(processed_data, f, indent=4)

create_and_upload_json(gcs_bucket_dag, filename, processed_data)

print(f"Processed data saved to {gcs_bucket_dag}/data/{filename}")
