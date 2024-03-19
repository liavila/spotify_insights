from urllib.parse import urlparse, parse_qs
import requests
from spotipy.oauth2 import SpotifyOAuth
from spotipy import Spotify
from dotenv import load_dotenv
from os import getenv
import json
import time
import spotipy

load_dotenv()

CLIENT_ID = getenv('SPOTIPY_CLIENT_ID')
CLIENT_SECRET = getenv('SPOTIPY_CLIENT_SECRET')
REDIRECT_URI = getenv('SPOTIPY_REDIRECT_URI')
SCOPE = 'user-read-recently-played user-top-read'


def get_authorization_code():
    unique_identifier = time.time()
    cache_path = f".cache-{unique_identifier}"
    sp_auth = SpotifyOAuth(client_id=CLIENT_ID,
                           redirect_uri=REDIRECT_URI,
                           scope=SCOPE,
                           cache_path=cache_path)

    print(
        f"Open the following URL in your browser: {sp_auth.get_authorize_url()}")
    redirected_url = input("Enter the URL you were redirected to: ")

    parsed_url = urlparse(redirected_url)
    code = parse_qs(parsed_url.query)['code'][0]

    sp_auth.get_access_token(code, as_dict=False)
    # sp_auth.get_cached_token()
    sp = Spotify(auth_manager=sp_auth)

    user = sp.current_user()
    print(f"Hi {user['display_name']}")

    cached_info = sp_auth.get_cached_token()
    print(cached_info)

    return user['id'], user['display_name'], cached_info


def save_user_info(user_id, name, info):
    # Create a dictionary of the user's information
    user_info = {
        'user_id': user_id,
        'name': name,
        'info': info
    }

    filename = 'user_information.json'

    try:
        # Try to load existing data from the file
        with open(filename, 'r') as file:
            data = json.load(file)
            # If the file exists and is not empty, append the new user's info
            data.append(user_info)
    except (FileNotFoundError, json.JSONDecodeError):
        # If the file does not exist or is empty, start a new list
        data = [user_info]

    # Write the updated list back to the file
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)

    print(f"User {name}'s information has been saved/updated in {filename}.")


def main():
    user_id, name, cached_info = get_authorization_code()
    save_user_info(user_id, name, cached_info)


if __name__ == '__main__':
    main()
