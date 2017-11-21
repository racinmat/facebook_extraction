import json

from facepy.utils import get_extended_access_token, get_application_access_token

if __name__ == '__main__':
    # You'll need an access token here to do anything.  You can get a temporary one
    # here: https://developers.facebook.com/tools/explorer/
    access_token = open('access_token.txt').readline()
    with open('credentials.json') as file:
        credentials = json.load(file)
        extended_key = get_extended_access_token(
            credentials['access_token'],
            credentials['app_id'],
            credentials['app_secret'],
            '2.11'
        )
        print(extended_key)