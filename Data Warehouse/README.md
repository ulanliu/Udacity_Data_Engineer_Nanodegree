# Project: Data Warehouse

Sparkify wants to analyze the data they've been collecting on songs and user activity and want to move their processes and data onto the cloud. This project is to build an ETL pipeline for a database hosted on AWS Redshift for Sparkify.

## Project Structure

Files used on the project:

1. `sql_queries.py` contains all the sql queries.
2. `create_tables.py` drops and creates the tables. Please run this file to reset your tables before each time you run the ETL scripts.
3. `etl.py` loads data from staging tables and re-insert them into the fact and dimension tables.
4. `test.ipynb` runs `create_tables.py` and `etl.py` and test the Redshift.
5. `dwh.cfg` contains AWS account informations.

## Project Steps

1. Run `create_tables.py` to drop the existing tables and create new tables.
2. Run `etl.py` to load data from staging tables and re-insert them into the fact and dimension tables.

## Database Schema

### Staging table

```
staging_events
	|- **artist** VARCHAR 
	|- **auth** VARCHAR
	|- **firstName** VARCHAR
	|- **gender** VARCHAR(1)
	|- **itemInSession** INT
	|- **lastName** VARCHAR
	|- **length** FLOAT
	|- **level** VARCHAR
	|- **location** TEXT
	|- **method** VARCHAR
	|- **page** VARCHAR
	|- **registration** VARCHAR
	|- **sessionId** INT
	|- **song** VARCHAR
	|- **status** INT    
	|- **ts** BIGINT
	|- **userAgent** TEXT
	|- **userId** TEXT        
```

```
staging_songs
	|- **song_id** VARCHAR 
	|- **num_songs** INT
	|- **artist_id** VARCHAR
	|- **artist_latitude** FLOAT
	|- **artist_longitude** FLOAT
	|- **artist_location** VARCHAR
	|- **artist_name** VARCHAR
	|- **title** VARCHAR
	|- **duration** FLOAT
	|- **year** INT
```

### Fact table

```
songplays
	|- **songplay_id** INT 	PRIMARY KEY
	|- **start_time** timestamp
	|- **user_id** int
	|- **level** text
	|- **song_id** text
	|- **artist_id** text
	|- **session_id** int
	|- **location** text
	|- **user_agent** text
```

### Dimension table

```
users
	|- **user_id** int  	PRIMARY KEY
	|- **first_name** text
	|- **last_name** text
	|- **gender** text
	|- **level** text
songs
	|- **song_id** VARCHAR 	PRIMARY KEY
	|- **title** text
	|- **artist_id** text
	|- **year** int
	|- **duration** float 
artists
	|- **artist_id** VARCHAR 	PRIMARY KEY
	|- **name** text
	|- **location** text
	|- **latitude** float
	|- **longitude** float
time
	|- **start_time** timestamp	PRIMARY KEY
	|- **hour** int
	|- **day** int
	|- **week** int
	|- **month** int
	|- **year** int
	|- **weekday** int
```
