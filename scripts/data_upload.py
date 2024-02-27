from pymongo import MongoClient
import json
from datetime import datetime

# Your MongoDB Atlas connection string
mongo_uri = "mongodb+srv://erenbardak:scWEeqdIh8oFZdnm@cluster0.5nawz27.mongodb.net/erenbardak?retryWrites=true&w=majority"
client = MongoClient(mongo_uri)

# Format the current date as a string in the format 'YYYY-MM-DD'
today_date_str = datetime.now().strftime('%Y-%m-%d')

# Include the date in the filename
filename_top_tracks = f'processed_top_tracks_data_{today_date_str}.json'

# Include the date in the filename
filename_recently_played = f'processed_recently_played_data_{today_date_str}.json'

# Select your database
db = client['projectspotify']

# Select the collections
collection_recently_played = db['rawdata_recentlyplayed']
collection_top_tracks = db['rawdata_toptracks']

# Load and upsert the recently played data
with open(filename_recently_played, 'r') as file:
    recently_played_data = json.load(file)
    collection_recently_played = db['rawdata_recentlyplayed']
    for item in recently_played_data:
        filter = {'user_id': item['user_id'],
                  'user_name': item['user_name'], 'played_at': item['played_at']}
        collection_recently_played.update_one(
            filter, {'$set': item}, upsert=True)

# Load and insert the top tracks data
with open(filename_top_tracks, 'r') as file:
    top_tracks_data = json.load(file)
collection_top_tracks.insert_many(top_tracks_data)

print("Data uploaded successfully to the respective collections in the projectspotify database.")

client.close()  # to ensure resources are released
