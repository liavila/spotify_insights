"""
This file is used to test various functions and methods before implementing them in the main program.
"""
import json
import time


def epoch_converter(epoch):
    """
    Convert an epoch time to a human-readable time
    :param epoch:
    :return: time in the format of 'YYYY-MM-DD HH:MM:SS'
    """
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch))


def print_recently_played(data='recently_played_data.json'):
    """
    Print the user's recently played tracks
    :param data: the file containing each user's recently played tracks
    """
    with open(data) as file:
        data = json.load(file)
        for user in data:
            print(f"User: {user['name']}")
            for song in user['recently_played']['items']:
                print(f"  {song['track']['name']} by {song['track']['artists'][0]['name']}")
                print(f'\tdate played: {song["played_at"]}')


def print_top_tracks(data='top_tracks_data.json'):
    """
    Print the user's top tracks
    :param data: A file containing each user's top tracks
    :return:
    """
    with open(data) as file:
        data = json.load(file)
        for user in data:
            print(f"User: {user['name']}")
            for song in user['top_tracks']['items']:
                print(f"  {song['name']} by {song['artists'][0]['name']}")


def print_expiration():
    """
    Print the expiration time of each user's access token
    """
    with open('user_information.json') as file:
        tokens = json.load(file)
        for user in tokens:
            print(f"{user['name']}'s access token expires at {epoch_converter(user['info']['expires_at'])}")



print('---converted epoch time: 1708810059---')
print(epoch_converter(1708810059))
print('---time now---')
print(epoch_converter(time.time()))
print('---time 24 hours ago---')
last_24 = int(time.time()) - 86400
print(epoch_converter(last_24))


print('recently played')
print_recently_played()

print('top tracks')
print_top_tracks()

