# Project: Data Modeling with Postgres

Sparkify wants to analyze the data they've been collecting on songs and user activity. This project is to create a Postgres database with tables and ETL pipeline for Sparkify who wants to analyze the data they've been collecting on songs and user activity to optimize queries on song play analysis.

## Project Structure

Files used on the project:

1. `data` contains song dataset and log dataset.
2. `sql_queries.py` contains all the sql queries.
3. `create_tables.py` drops and creates the tables. Please run this file to reset your tables before each time you run the ETL scripts.
4. `test.ipynb` displays the first few rows of each table to let you check your database.
5. `etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into the tables.
6. `etl.py` reads and processes files from song_data and log_data and loads them into the tables.

## Project Steps

1. Run `create_tables.py` to drop the existing tables and create new tables.
2. Run `etl.py` to process files from song_data and log_data and loads them into the tables.
3. Use `test.ipynb` to test the database.

## Database Schema

### Fact table

```
songplays
	|- **songplay_id** SERIAL 	PRIMARY KEY
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