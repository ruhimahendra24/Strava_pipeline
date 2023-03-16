import os
import json
import requests
import pandas as pd
import time 
import pendulum
from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


## DEFINE DAG
@dag(dag_id = 'strava_activities',
     schedule_interval = '@daily',
     start_date = pendulum.datetime(2023,3,7),
     catchup = False
)


def strava_summary(): 
    ## CREATE EMPTY DB
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
        ## GET STRAVA ACTIVITIES FROM API
        def get_activities():
        
            with open('/Users/ruhimahendra/airflow/dags/strava_creds.json') as creds:
                strava_creds = json.load(creds)
                     
            response = requests.post(
                            url = 'https://www.strava.com/oauth/token',
                            data = strava_creds
                        )
            strava_tokens = response.json()
    
            #Loop through all activities
            page = 1
            url = "https://www.strava.com/api/v3/activities"
            print(strava_tokens)
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
            return activities.to_json(orient='records')
        strava_activies = get_activities()
                    
        create_database.set_downstream(strava_activies)
    
    
        @task()
        ## LOAD ACTIVITIES TO DB
        def load_activities(activties):
            hook = SqliteHook(sqlite_conn_id="activities")
            stored_activties = hook.get_pandas_df("SELECT * from runs;")
            new_activties = []
            activties = json.loads(activties)
            for activity in activties:
                if str(activity["id"]) not in stored_activties["id"].values:
                    new_activties.append([activity['id'],activity['name'],activity['start_date_local'],activity['type'],activity['distance'],activity['moving_time'],activity['elapsed_time'],activity['total_elevation_gain'],activity['end_latlng'],activity['external_id']])
            hook.insert_rows(table='runs', rows=new_activties, target_fields=["id", "name", "start_date_local", "type", "distance", "moving_time", "elapsed_time", "total_elevation_gain", "end_latlng", "external_id"])
            return new_activties

        new_activties = load_activities(strava_activies)
        
    
summary = strava_summary()
