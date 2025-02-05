# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv

# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)

# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))

# CREATING A CLUSTER
from cassandra.cluster import Cluster
cluster = Cluster()

# CREATING THE SESSION
session = cluster.connect()

# SESSION EXECUTION
session.execute("""
      CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
""")

# SETTING THE KEYSPACE
session.set_keyspace("mykeyspace")

## Create queries to ask the following three questions of the data

### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4


### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
    

### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

# HERE WE ARE CREATING THE MUSIC_HISTORY_FINALVER 
# WE HAVE SESSION_ID & ITEM_IN_SESSION AS THE PRIMARY KEYS BECAUSE WE WANT TO FILTER OUT TO SESSION_ID & ITEM_IN_SESSION 
table1 = """CREATE TABLE IF NOT EXISTS music_history_finalver1 (
session_id int, item_in_session int, artist text, song_title text, song_length float, 
PRIMARY KEY(session_id, item_in_session))"""         

# TABLE1 EXEUCTION
session.execute(table1)

file = 'event_datafile_new.csv'

# WE ARE LOOPING THROUGH THE CSV
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        # WE ARE INSERTING THE DATA FROM THE CSV INTO THE MUSIC_HISTORY_FINALVER TABLE
        query = "INSERT INTO music_history_finalver1 (session_id, item_in_session, artist, song_title, song_length) VALUES (%s,%s,%s,%s,%s)"
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[10], float(line[5])))
# THE QUERY WE WANNT TO EXECUTE 
verification = "SELECT artist, song_title,song_length FROM music_history_finalver1 WHERE session_id = 338 and item_in_session = 4" 

# CREATING LISTS TO CREATE A DATAFRAME FOR THE RESULTS LATER ON
artist = []
song_title = []
song_length = [] 

# EXECUTING THE RESULTS AND APPENDING THE DATA INTO THE CORRECT LISTS ABOVE
try:
    results = session.execute(verification)
    for row in results:
        artist.append(row[0])
        song_title.append(row[1])
        song_length.append(row[2])
except Exception as error:
    print(error)

# CREATING A DATAFRAME FOR THE RESULTS 
try: 
    df = pd.DataFrame({
        "artist":artist,
        "song_title":song_title,
        "song_length": song_length
    })
    print(df)
    #print(df(index=False)) -> WE CAN RUN THE RESULTS WITHOUT SEEING THE INDEX IF REQUIRED WHICH WILL CONVER THE DISPLAYED RESULTS INTO A STRING
except Exception as error:
    print(error)

# WE ARE CREATING TABLE 2 (USER_INFORMATION_FINALVER)
# WE ARE USING USER_ID AS THE PARTITION KEY WITH SESSION_ID AND ITEM_IN_SESSION BEING THE CLUSTERING COLUMNS TO CREATE THE UNIQUE PRIMARY KEY

table2 = """CREATE TABLE IF NOT EXISTS user_information_finalver (user_id int, session_id int, item_in_session int, artist text, song text, user_first_name text, user_last_name text,
PRIMARY KEY(user_id, session_id, item_in_session))
"""      
# TABLE2 EXECUTION
session.execute(table2)

file = 'event_datafile_new.csv'

# LOOPING THROUGH THE CSV
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        # WE ARE INSERTING THE DATA FROM THE CSV INTO THE USER_INFORMATION_FINALVER TABLE
        query = "INSERT INTO user_information_finalver (user_id, session_id, item_in_session, artist, song, user_first_name, user_last_name) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))

# QUERY WE WANT TO EXECUTE
verification2 = "SELECT artist, song, user_first_name, user_last_name FROM user_information_finalver WHERE user_id = 10 and session_id = 182"

# CREATING LISTS TO CREATE A DATAFRAME FOR THE RESULTS LATER ON
artist = []
song = []
user_first_name = []
user_last_name = []
try:
    # EXECUTING THE RESULTS AND APPENDING THE DATA INTO THE CORRECT LISTS ABOVE
    results = session.execute(verification2)
    for rows in results:
        artist.append(row[0])
        song.append(rows[1]) 
        user_first_name.append(rows[2])
        user_last_name.append(rows[3])
except Exception as error:
    print(error)

# CREATING A DATAFRAME FOR THE RESULTS
try: 
    df = pd.DataFrame({
    "artist":artist,
    "song" : song, 
    "user_first_name":user_first_name,
    "user_last_name": user_last_name
    })
    print(df)
    #print(df(index=False)) -> WE CAN RUN THE RESULTS WITHOUT SEEING THE INDEX IF REQUIRED WHICH WILL CONVER THE DISPLAYED RESULTS INTO A STRING

except Exception as error: 
    print(error)

# CREATING THE LISTENING_HISTORY_FINALVER TABLE 
# THE PRIMARY KEYS ARE SONG AND USER_ID. SONG COMES FIRST BECAUSE WE WANT TO FILTER OUT TO SONGS AND USER ID IS THE CLUSTERING COLUMN TO ORDER THE DATA WITHIN EACH SONG PARTITION

table3 = """CREATE TABLE IF NOT EXISTS listening_history_finalver (song text, user_id int, user_first_name text, user_last_name text,
PRIMARY KEY(song, user_id))
"""      

# TABLE3 EXECUTION
session.execute(table3)

file = 'event_datafile_new.csv'

# LOOPING THROUGH THE CSV
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO listening_history_finalver (song, user_id, user_first_name, user_last_name) VALUES (%s,%s,%s,%s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))

# QUERY WE WANT TO EXECUTE
verification3 = "SELECT user_first_name, user_last_name FROM listening_history_finalver WHERE song = 'All Hands Against His Own'"

# CREATING LISTS TO CREATE A DATAFRAME FOR THE RESULTS LATER ON
user_first_name = []
user_last_name = []

try:
    # EXECUTING THE RESULTS AND APPENDING THE DATA INTO THE CORRECT LISTS ABOVE
    results = session.execute(verification3)
    for rows in results:
        user_first_name.append(rows[0])
        user_last_name.append(rows[1])
except Exception as error:
    print(error)
    
# CREATING A DATAFRAME FOR THE RESULTS 
try:
    df = pd.DataFrame({
        "user_first_name":user_first_name,
        "user_last_name":user_last_name
    })
    print(df)
    #print(df(index=False)) -> WE CAN RUN THE RESULTS WITHOUT SEEING THE INDEX IF REQUIRED WHICH WILL CONVER THE DISPLAYED RESULTS INTO A STRING

except Exception as error:
    print(error)

# DROPPING ALL THE TABLES
session.execute("DROP TABLE music_history_finalver1;")
session.execute("DROP TABLE user_information_finalver;")
session.execute("DROP TABLE listening_history_finalver;")

# CLOSING THE SESSION & CLUSTER
session.shutdown()
cluster.shutdown()
