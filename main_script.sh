#!/bin/bash

cd /Users/irerielunicornio/Documents/USF/Spring1/Distributed-Data-Systems/Final\ Project/scripts

# Run spotify_script.py
python data_fetcher.py

# Wait for 15 seconds
sleep 15

# Run recently_played_json_converter.py 
python recently_played_json_converter.py

# Wait for another 3 seconds
sleep 3

# Run top_tracks_json_converter.py
python top_tracks_json_converter.py

# Wait for another 3 seconds
sleep 3

# Add data to GCS
python add_data_to_gcs.py

# # Run data_upload.py Uploads the processed_recently_played.json to mongodb atlas
# python data_upload.py

# # Creates 5 collections: "Albums, Tracks, ArtistTrackLink, Artists, Image" from "rawdata_recentlyplayed" collection
# python create_collections_recently_played.py

# # Wait for another 5 seconds
# sleep 3

# # Creates 3 collections: "t50_Artists, t50_Tracks, t50_Images" from "rawdata_toptracks" collection
# python create_collections_top_tracks.py