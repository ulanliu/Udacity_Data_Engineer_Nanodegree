# Project: Data Warehouse

Sparkify has grown their user base and song database even more and want to move their data warehouse to a data lake. This project is to build an ETL pipeline that extracts their data from S3, process them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Project Structure

Files used on the project:
1. `etl.py` extracts the data from S3, process them with Spark and loads them back into S3.
2. `dl.cfg` contains AWS account informations.

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