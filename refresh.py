# This portion was not written by me, but is integral to the rest of the code

import requests
import datetime
from oauth2client.client import OAuth2Credentials

oauth_params = dict(
    client_id='client-id-here'
    client_secret='client-secret-here',
    scope=["https://www.googleapis.com/auth/youtube.readonly",
           "https://www.googleapis.com/auth/yt-analytics.readonly"],
    access_type='offline'
)

oauth_credentials = {
    "access_token": 'access-token-here',
    "refresh_token": 'refresh-token-here'
}

refresh_params = {
    'client_id': oauth_params['client_id'],
    'refresh_token': oauth_credentials['refresh_token'],
    'client_secret': oauth_params['client_secret'],
    'grant_type': 'refresh_token'
}


def oauth_credentials():
    rs = requests.post('https://accounts.google.com/o/oauth2/token', refresh_params).json()
    expiry = datetime.datetime.now() + datetime.timedelta(seconds=rs['expires_in'])
    cred_params = {
        'access_token': rs['access_token'],
        'client_id': oauth_params['client_id'],
        'client_secret': oauth_params['client_secret'],
        'refresh_token': refresh_params['refresh_token'],
        'token_expiry': expiry,
        'token_uri': 'https://accounts.google.com/o/oauth2/token',
        'user_agent': '',
    }

    return OAuth2Credentials(**cred_params)
