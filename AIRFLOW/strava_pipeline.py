#%% 
import os
import json
import requests
import pandas as pd
import time 

import pendulum
from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

#%%
@dag(dag_id = 'strava_activities',
     schedule_interval = '@hourly',
     start_date = pendulum.datetime(2023,3,7),
     catchup = False
)
def strava_summary(): 
    
        create_database = SqliteOperator(
            task_id = "create_table_sqlite",
             sql=r"""
        CREATE TABLE IF NOT EXISTS runs (
            'id' TEXT PRIMARY KEY,
            'name' TEXT,
            'start_date_local' TEXT,
            'type' TEXT,
            'distance' TEXT,
            'moving_time' TEXT,
            'elapsed_time' TEXT,
            'total_elevation_gain' TEXT,
           'end_latlng' TEXT,
            'external_id' TEXT
        );
        """,
        sqlite_conn_id="activities"
    )

        @task()
        def get_activities():
        # OPEN RESPONSE FILE
            strava_tokens = {"token_type": "Bearer", "expires_at": 1677391330, "expires_in": 21600, "refresh_token": "223a205b1cdb8d651d32eda3b895dcac0a52035b", "access_token": "effd4c5a975d45116d97ae22adf0120ce8714e8a", "athlete": {"id": 57262801, "username": "ruhi_mahendra", "resource_state": 2, "firstname": "Ruhi", "lastname": "Mahendra", "bio": "", "city": "Montreal", "state": "Quebec", "country": "Canada", "sex": "F", "premium": True, "summit": True, "created_at": "2020-05-05T22:34:09Z", "updated_at": "2023-01-22T18:54:31Z", "badge_type_id": 1, "weight": 49.8, "profile_medium": "https://dgalywyr863hv.cloudfront.net/pictures/athletes/57262801/21042381/12/medium.jpg", "profile": "https://dgalywyr863hv.cloudfront.net/pictures/athletes/57262801/21042381/12/large.jpg", "friend": None, "follower": None}}
            ## If access_token has expired then use the refresh_token to get the new access_token
            if strava_tokens['expires_at'] < time.time():
            #Make Strava auth API call with current refresh token
                response = requests.post(
                                    url = 'https://www.strava.com/oauth/token',
                                    data = {
                                            'client_id': 79117,
                                            'client_secret': '96a73b79661036fa04c67697846e22d40789b21a',
                                            'grant_type': 'refresh_token',
                                            'refresh_token': strava_tokens['refresh_token']
                                            }
                                )
            #Save response as json in new variable
                new_strava_tokens = response.json()
    
            #Use new Strava tokens from now
                strava_tokens = new_strava_tokens
            #Loop through all activities
            page = 1
            url = "https://www.strava.com/api/v3/activities"
            access_token = strava_tokens['access_token']
            ## Create the dataframe ready for the API call to store your activity data
            activities = pd.DataFrame(
                columns = [
                        "id",
                        "name",
                        "start_date_local",
                        "type",
                        "distance",
                        "moving_time",
                        "elapsed_time",
                        "total_elevation_gain",
                        "end_latlng",
                        "external_id"
                ]
            )
            while True:
                
                # get page of activities from Strava
                r = requests.get(url + '?access_token=' + access_token + '&per_page=200' + '&page=' + str(page))
                r = r.json()
            # if no results then exit loop
                if (not r):
                    break
                
                # otherwise add new data to dataframe
                for x in range(len(r)):
                    activities.loc[x + (page-1)*200,'id'] = r[x]['id']
                    activities.loc[x + (page-1)*200,'name'] = r[x]['name']
                    activities.loc[x + (page-1)*200,'start_date_local'] = r[x]['start_date_local']
                    activities.loc[x + (page-1)*200,'type'] = r[x]['type']
                    activities.loc[x + (page-1)*200,'distance'] = r[x]['distance']
                    activities.loc[x + (page-1)*200,'moving_time'] = r[x]['moving_time']
                    activities.loc[x + (page-1)*200,'elapsed_time'] = r[x]['elapsed_time']
                    activities.loc[x + (page-1)*200,'total_elevation_gain'] = r[x]['total_elevation_gain']
                    activities.loc[x + (page-1)*200,'end_latlng'] = r[x]['end_latlng']
                    activities.loc[x + (page-1)*200,'external_id'] = r[x]['external_id']
            # increment page
                page += 1
            return activities.to_json()
        strava_activies = get_activities()
                    
        create_database.set_downstream(strava_activies)
summary = strava_summary()