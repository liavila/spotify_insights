from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os

# Your MongoDB Atlas connection string
# Define it in the environment variables
mongo_uri = os.getenv("MONGO_URI")

client = MongoClient(mongo_uri, server_api=ServerApi('1'))

# Select your database
db = client['Spotify-dds2024']

# Raw data collection
rawdata_collection = db['rawdata_recentlyplayed']

# New collections
artists_collection = db['Artists']
albums_collection = db['Albums']
tracks_collection = db['Tracks']
artist_track_link_collection = db['ArtistTrackLink']
images_collection = db['Images']

# Extract and insert unique artists with their Spotify URLs and IDs
for artist in rawdata_collection.aggregate([
    {"$unwind": "$artists_spotify_url"},
    {"$unwind": "$artists_id"},
    {"$group": {
        "_id": "$artists_id",
        "artist_name": {"$first": "$artist_names"},
        "spotify_url": {"$first": "$artists_spotify_url"},
        "user_id": {"$first": "$user_id"},
        "user_name": {"$first": "$user_name"}
    }}
]):
    artists_collection.insert_one({
        'user_id': artist['user_id'],
        'user_name': artist['user_name'],
        'artist_id': artist['_id'],
        'artist_name': artist['artist_name'],
        'spotify_url': artist['spotify_url']
    })

# Extract and insert unique albums with total_tracks
for album in rawdata_collection.aggregate([
    {"$group": {
        "_id": "$album_id",
        "album_name": {"$first": "$album_name"},
        "release_date": {"$first": "$release_date"},
        "total_tracks": {"$first": "$total_tracks"},
        "user_id": {"$first": "$user_id"},
        "user_name": {"$first": "$user_name"}
    }}
]):
    albums_collection.insert_one({
        'user_id': album['user_id'],
        'user_name': album['user_name'],
        'album_id': album['_id'],
        'album_name': album['album_name'],
        'release_date': album['release_date'],
        'total_tracks': album['total_tracks']
    })

# Insert tracks with additional info including user_id and user_name
for track in rawdata_collection.find():
    tracks_collection.insert_one({
        'user_id': track['user_id'],
        'user_name': track['user_name'],
        'track_id': track['track_id'],
        'album_id': track['album_id'],
        'track_name': track['track_name'],
        'artist_song_name': track['artist_song_name'],
        'duration': track['duration'],
        'popularity': track['popularity'],
        'explicit': track['explicit'],
        'release_date': track['release_date'],
        'danceability': track.get('danceability'),
        'energy': track.get('energy'),
        'key': track.get('key'),
        'loudness': track.get('loudness'),
        'mode': track.get('mode'),
        'speechiness': track.get('speechiness'),
        'acousticness': track.get('acousticness'),
        'instrumentalness': track.get('instrumentalness'),
        'liveness': track.get('liveness'),
        'valence': track.get('valence'),
        'tempo': track.get('tempo'),
        'time_signature': track.get('time_signature')
    })

# For each track, link artists and additional info including user_id and user_name
for track in rawdata_collection.find():
    for artist_id in track['artists_id']:
        artist_track_link_collection.insert_one({
            'user_id': track['user_id'],
            'user_name': track['user_name'],
            'artist_id': artist_id,
            # Assuming one artist name per track
            'artist_name': track['artist_names'],
            'track_id': track['track_id'],
            'track_name': track['track_name'],
            'played_at': track['played_at'],
            'is_local': track['is_local'],
            'context_type': track.get('context_type', 'none'),
            'preview_url': track['preview_url']
        })

    # Insert images with user_id and user_name
    for image in track['images']:
        images_collection.insert_one({
            'user_id': track['user_id'],
            'user_name': track['user_name'],
            'track_id': track['track_id'],
            'url': image['url'],
            'height': image['height'],
            'width': image['width']
        })

print("Data processed and inserted into Artists, Albums, Images, Tracks, ArtistTrackLink collections.")

client.close()  # to ensure resources are released
