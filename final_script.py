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
print(full_data_rows_list)

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

# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()

# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()

# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()

## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
table1 = """CREATE TABLE IF NOT EXISTS music_history2 (
session_id int, item_in_session text, artist text, song_title text, song_length text, 
PRIMARY KEY(session_id, item_in_session))"""         

session.execute(table1)

# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO music_history2 (session_id, item_in_session, artist, song_title, song_length) VALUES (%s,%s,%s,%s,%s)"
        #query = query + "(%s,%s,%s,%s,%s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (line[9], line[3], line[0], line[10], line[5]))

# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

## TO-DO: Add in the SELECT statement to verify the data was entered into the table
verification = "SELECT artist, song_title,song_length FROM music_history2 WHERE session_id = '338' and item_in_session = '4'" 

try:
    results = session.execute(verification)
    print(results)
except Exception as error:
    print(error)

## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182

table2 = """CREATE TABLE IF NOT EXISTS user_information (user_id text, session_id text, artist text, song text, user_first_name text, user_last_name text,
PRIMARY KEY(user_id, session_id))
WITH CLUSTERING ORDER BY (session_id desc); 
"""      
session.execute(table2)

# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO user_information (user_id, session_id, artist, song, user_first_name, user_last_name) VALUES (%s,%s,%s,%s,%s,%s)"
        #query = query + "(%s,%s,%s,%s,%s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (line[10], line[8], line[0], line[9], line[1], line[4]))
        
verification2 = "SELECT artist, song, user_first_name, user_last_name FROM user_information WHERE user_id = '10' and session_id = '182'"
try:
    results = session.execute(verification2)
    print(results)
except Exception as error:
    print(error)

## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'


table3 = """CREATE TABLE IF NOT EXISTS listening_history (user_first_name text, user_last_name text, song text,
PRIMARY KEY(song))
"""      
session.execute(table3)

# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO listening_history (user_first_name, user_last_name, song) VALUES (%s,%s,%s)"
        #query = query + "(%s,%s,%s,%s,%s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (line[9], line[1], line[4]))
        
verification3 = "SELECT user_first_name, user_last_name FROM listening_history WHERE song = 'All Hands Against His Own'"
try:
    results = session.execute(verification3)
    print(results)
except Exception as error:
    print(error)

session.execute("DROP TABLE music_history2;")
session.execute("DROP TABLE user_information;")
session.execute("DROP TABLE listening_history;") 

session.shutdown()
cluster.shutdown()
