from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os

# MongoDB Atlas connection string
# Define it in the environment variables
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri, server_api=ServerApi('1'))

# Database setup
db = client['Spotify-dds2024']

rawdata_toptracks = db['rawdata_toptracks']
t50_tracks_collection = db['t50_Tracks']
t50_images_collection = db['t50_Images']
t50_artists_collection = db['t50_Artists']

# Fetch and process each document from rawdata_toptracks
for doc in rawdata_toptracks.find():
    # Insert track
    t50_tracks_collection.insert_one({
        'user_id': doc['user_id'],
        'user_name': doc['user_name'],
        'song_id': doc['song_id'],
        'artist_ids': doc['artist_ids'],
        'top_track_song_name': doc['top_track_song_name'],
        'rank': doc['rank'],
        'artist_names': doc['artist_names'],
        'song_link': doc['song_link'],
        'danceability': doc.get('danceability'),
        'energy': doc.get('energy'),
        'key': doc.get('key'),
        'loudness': doc.get('loudness'),
        'mode': doc.get('mode'),
        'speechiness': doc.get('speechiness'),
        'acousticness': doc.get('acousticness'),
        'instrumentalness': doc.get('instrumentalness'),
        'liveness': doc.get('liveness'),
        'valence': doc.get('valence'),
        'tempo': doc.get('tempo'),
        'time_signature': doc.get('time_signature'),
        'date_data_fetched': doc['date_data_fetched']
    })

    # Insert artists
    for artist_id in doc['artist_ids']:
        # Assuming artist_names is a single string. If it's an array, adjust accordingly.
        t50_artists_collection.update_one(
            {'artist_id': artist_id},
            {'$setOnInsert': {
                'user_id': doc['user_id'],
                'user_name': doc['user_name'],
                'artist_names': doc['artist_names']
            }},
            upsert=True
        )

    # Insert images
    for image in doc['images']:
        t50_images_collection.insert_one({
            'user_id': doc['user_id'],
            'user_name': doc['user_name'],
            'song_id': doc['song_id'],
            'url': image['url'],
            'height': image['height'],
            'width': image['width']
        })

print("Data processed and inserted into t50_Tracks, t50_Artists, and t50_Images collections.")

client.close()  # to ensure resources are released
