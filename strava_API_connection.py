# IMPORT LIBRARIES
import requests
import json

# CREDENTIALS
client_id = 79117
client_secret = '96a73b79661036fa04c67697846e22d40789b21a'
redirect_url = 'http://localhost/'
code = '41d9decced7edc397c32376e53c24e72ef13787e'
  
# REQUEST URL RESPONSE
response = requests.post(
                    url = 'https://www.strava.com/oauth/token',
                    data = {
                            'client_id': client_id,
                            'client_secret': client_secret,
                            'code': code,
                            'grant_type': 'authorization_code'
                            }
                )

# SAVE JSON RESPONSE
strava_tokens = response.json()
with open('strava_tokens.json', 'w') as outfile:
    json.dump(strava_tokens, outfile)
