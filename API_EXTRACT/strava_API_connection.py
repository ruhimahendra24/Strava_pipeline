# IMPORT LIBRARIES
import requests
import json

# CREDENTIALS
key_file = open('/Users/ruhimahendra/Desktop/Strava_pipeline/API_EXTRACT/access.txt')
lines = key_file.readlines()

# REQUEST URL RESPONSE
response = requests.post(
                    url = 'https://www.strava.com/oauth/token',
                    data = {
                            'client_id': lines[0].split(':')[1].rstrip(),
                            'client_secret': lines[1].split(':')[1].rstrip(),
                            'code': lines[2].split(':')[1].rstrip(),
                            'grant_type': lines[3].split(':')[1].rstrip()
                            }
                )

# SAVE JSON RESPONSE
strava_tokens = response.json()
with open('strava_tokens.json', 'w') as outfile:
    json.dump(strava_tokens, outfile)
