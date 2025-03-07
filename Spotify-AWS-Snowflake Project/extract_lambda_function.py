"""Spotyfy data extract lambda function."""

import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # generate spotify credententials by creating an app 
    # using spotify's developer site -> https://developer.spotify.com/
    # spotipy docs -> https://spotipy.readthedocs.io/en/2.25.1/#
    # Add client_id and client_secret in env
    cilent_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=cilent_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp.user_playlists('spotify')
    
    playlist_URI = "https://open.spotify.com/playlist/3bDJLJzvUBxBV4C7mezz6p"
    playlist_id = playlist_URI.split('/')[-1]
    playlists_result = sp.playlist(playlist_id)
    
    cilent = boto3.client('s3')
    
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    cilent.put_object(
        Bucket="aws-de-s3-bucket-1807",
        Key="raw/to_process/" + filename,
        Body=json.dumps(playlists_result)
        )
    
    print("**** data uploaded to s3 ****")