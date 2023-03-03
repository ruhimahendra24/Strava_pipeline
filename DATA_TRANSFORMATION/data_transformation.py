# %%
#import libraries
import pandas as pd 

# %%
#import csv file
df = pd.read_csv('strava_activities_all_fields.csv')
# %%
# filter data just to look at run activities 
df = df[df['sport_type'] == 'Run']

# %%
df