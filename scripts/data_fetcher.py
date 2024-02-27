import json
from spotipy.oauth2 import SpotifyOAuth
import spotipy
from dotenv import load_dotenv
from pathlib import Path
import os
import time

load_dotenv()

client_id = os.getenv('SPOTIPY_CLIENT_ID')
client_secret = os.getenv('SPOTIPY_CLIENT_SECRET')
redirect_uri = os.getenv('SPOTIPY_REDIRECT_URI')

PATH = Path("/Users/irerielunicornio/Documents/USF/Spring1/Distributed-Data-Systems/Final Project/scripts")

def load_user_tokens():
    with open("user_information.json") as file:
        return json.load(file)


def refresh_access_token(user_info, client_id, redirect_uri):
    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope=user_info['info']['scope']
    )
    new_token_info = auth_manager.refresh_access_token(
        user_info['info']['refresh_token'])
    return new_token_info


def get_recommendations_for_track(sp, seed_tracks):
    recommendations = sp.recommendations(seed_tracks=seed_tracks, limit=5)
    recommended_tracks = []
    for track in recommendations['tracks']:
        recommended_tracks.append({
            'name': track['name'],
            'artists': ', '.join(artist['name'] for artist in track['artists']),
            'id': track['id']
        })
    return recommended_tracks


def get_audio_features(sp, tracks):
    track_ids = [track['track']['id'] for track in tracks['items']]
    audio_features = sp.audio_features(track_ids)
    for i, track in enumerate(tracks['items']):
        # Combine track info with its audio features
        track['audio_features'] = audio_features[i]
    return tracks


def get_recently_played(sp):
    """Get the user's recently played tracks,
    up to 50 tracks, from the Spotify API
    This grabs songs from the last 24 hours"""
    last_24 = int(time.time()) - 86400
    tracks_listened_in_last_24 = sp.current_user_recently_played(after=last_24)
    tracks_with_features = get_audio_features(sp, tracks_listened_in_last_24)
    return tracks_with_features


def fetch_recommendations_for_track(sp, track_id):
    try:
        recommendations = sp.recommendations(seed_tracks=track_id, limit=5)
        return [{
            'song_id': track['id'],
            'song_name': track['name'],
            'artist_names': ', '.join(artist['name'] for artist in track['artists'])
        } for track in recommendations['tracks']]
    except Exception as e:
        print(f"Failed to fetch recommendations for track ID {track_id}: {e}")
        return []


def get_top_tracks(sp):
    top_tracks = sp.current_user_top_tracks()
    track_ids = [track['id'] for track in top_tracks['items']]
    audio_features = sp.audio_features(track_ids)
    # Combine the top tracks and audio features based on track IDs
    for track, features in zip(top_tracks['items'], audio_features):
        track['audio_features'] = features

    return top_tracks


def main():
    tokens = load_user_tokens()
    client_id = os.getenv('CLIENT_ID')
    redirect_uri = os.getenv('REDIRECT_URI')
    recently_played_data = []
    top_tracks_data = []

    for user in tokens:
        current_time = int(time.time())
        # Check if token needs to be refreshed
        # print(f'The current time is {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(current_time))}')
        # print(f"{user['name']}'s access token expires at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(user['info']['expires_at']))}")
        if 'expires_at' not in user['info'] or user['info']['expires_at'] < current_time:
            new_token_info = refresh_access_token(
                user, client_id, redirect_uri)
            # print(f'{new_token_info["access_token"]} expires at {new_token_info["expires_in"]}')
            user['info']['access_token'] = new_token_info['access_token']
            user['info']['expires_at'] = current_time + \
                new_token_info['expires_in']

        # print(f"{user['name']}'s access token expires at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(user['info']['expires_at']))}")

        # Use the valid or refreshed access token
        sp = spotipy.Spotify(auth=user['info']['access_token'])
        recently_played = get_recently_played(sp)
        recently_played_data.append({
            "user_id": user['user_id'],
            "name": user['name'],
            "recently_played": recently_played
        })

        top_tracks = get_top_tracks(sp)
        top_tracks_data.append({
            "user_id": user['user_id'],
            "name": user['name'],
            "top_tracks": top_tracks
        })

    # SAVE A NEW FILE EVERY TIME?

    # Save the recently played data to a JSON file
    with open(PATH/'recently_played_data.json', 'w') as file:
        json.dump(recently_played_data, file, indent=4)

    # Save the top tracks data to a JSON file
    with open(PATH/'top_tracks_data.json', 'w') as file:
        json.dump(top_tracks_data, file, indent=4)

    # Update the original tokens with the refreshed information
    with open(PATH/'user_information.json', 'w') as file:
        json.dump(tokens, file, indent=4)

    print('Data has been fetched and saved to recently_played_data.json and top_tracks_data.json')


if __name__ == "__main__":
    main()
