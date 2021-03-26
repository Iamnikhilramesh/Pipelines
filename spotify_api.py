#required libraries 
import datetime
import requests
import pandas as pd
import numpy as np 
import sqlalchemy
import sqlite3
import base64


#Define/ Settings

client_id = '7daab2d85efc4ab6b987f6d5e134fd83'
client_secret = '7da4be2ffe9e49328f67dc5c4c0bf951'
client_creds = f"{client_id}:{client_secret}"
#convert string to  base 64 encode
client_creds_b64 = base64.b64encode(client_creds.encode())

#Define
token_url = "https://accounts.spotify.com/api/token"
token_data = {
    "grant_type": "client_credentials"
}
token_headers = {
    "Authorization": f"Basic {client_creds_b64.decode()}" 
    # <base64 encoded client_id:client_secret>
}

#Call a request to get the token
r = requests.post(token_url, data=token_data, headers=token_headers)
tok = r.json()
token = tok['access_token']
print(token)


#Settings
database_location = "sqlite://tracks.sqlite"

headers = {
    "Accept" : "application/json",
    "Content-Type" : "application/json",
    "Authorization": "Bearer {token}".format(token=token)
}
today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=100)
yesterday_unix = int(yesterday.timestamp()) * 1000

# Request url to get the data from spotify 
r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = yesterday_unix), headers = headers)

#print the data 
#print(r.json())

#data
raw_data = r.json()

song_name = []
artist_name = []
album_name = []
date_played = []
time_stamp = []
for song in raw_data["items"]:
    song_name.append(song["track"]["name"])
    artist_name.append(song["track"]["album"]["artists"][0]["name"])
    date_played.append(song["played_at"])  
    time_stamp.append(song["played_at"][0:10])

l = [song_name, artist_name, date_played , time_stamp]
col_names = ['song_name', 'artist_name', 'date_played','time_stamp']

#The extracted data is converted into Dataframe
df = pd.DataFrame (l).transpose()
df.columns = col_names

#transform function
if df.empty:
    print("No Songs Downloaded!!!!!!!!!!")

if df["date_played"].unique:
    pass
else:
    raise Exception("Duplicate values are available")
yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
yesterday = yesterday.replace(hour=0,minute=0,second=0,microsecond=0)
time_stamps = df["time_stamp"].tolist()
# for timestamp in time_stamps:
#     if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
#         raise Exception("Not all songs are downloaded!!!!!")

#Sql quesry to create a table 
sql_create_table = """ 
CREATE TABLE IF NOT EXISTS played_tracks(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    album_name VARCHAR(200),
    date_played VARCHAR(200),
    time_stamp VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (date_played) 
)
"""

#Three steps to create a database
# 1 Create engine at the database location 
engine = sqlalchemy.create_engine("sqlite+pysqlite:///:memory:")
# 2 Connect to the database
conn = sqlite3.connect("new_track.sqlite")
# 3 Create a cursor to run the sql scripts
cursor = conn.cursor()
#Execute the cursor sql from the created cursor
cursor.execute(sql_create_table)
run =  """ SELECT * from played_tracks;"""
#load data into the database
try:
    df.to_sql("played_tracks",engine, index=False, if_exists="append")
    data = cursor.execute(run)
    print(data)

except Exception as e:
    print(e)
conn.close()



#Commands to install Airflow
# virtualenv airflow
# source airflow/bin/activate
# pip freeze
# export AIRFLOW_HOME=~/airflow
# pip install apache-airflow
# pip install marshmallow==2.19
# pip install requests==2.8