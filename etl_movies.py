# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 02:41:37 2023
"""

import pandas as pd
import numpy as np
import mysql.connector
from google.cloud import bigquery
from mysql.connector import errorcode

def get_movie_rating(v):
    if v < 3:
        return 'bad'
    elif v < 6:
        return 'okay'
    else:
        return 'good'

def get_watchability(d):
    if d < 60:
        return 'short movie'
    elif d < 90:
        return 'avg length movie'
    elif d < 5000:
        return 'really long movie'
    else:
        return 'no data'

## Connceting Python to Oscar's MySQL credentials

try:
    conn = mysql.connector.connect(
        host = 'oscarvalles.com',
        user = 'oscarval_user',
        password = 'learnsql123',
        database = 'oscarval_sql_course'
    )
    print('Connection Successful')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Check Credentials')
    else:
        print('Error')

query = """
        select year, title, genre, avg_vote, duration
        from oscarval_sql_course.imdb_movies
        where year between 2005 and 2010;
        """

df = pd.read_sql(query, conn)
df['movie_rating'] = [get_movie_rating(v) for v in df['avg_vote']]
df['watchability'] = [get_watchability(d) for d in df['duration']]

conn.close()

## Load Data to BQ
proj = 'udemy-project-374902'
dataset = 'sample_dataset'
target_table = 'annual_movie_summary'
table_id = f'{proj}.{dataset}.{target_table}'

client = bigquery.Client(project=proj)

job_config = bigquery.LoadJobConfig(
    autodetect=True,
    write_disposition='WRITE_TRUNCATE'
)

load_job = client.load_table_from_dataframe(dataframe=df, destination=table_id, job_config=job_config)
load_job.result()

dest_table = client.get_table(table=table_id)
print(f"You have {dest_table.num_rows} in your BQ table.")