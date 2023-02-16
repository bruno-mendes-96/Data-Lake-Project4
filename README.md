# Project - Data Lake

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this context, an ETL pipeline was developed; the pipeline extracts their data from S3, proccesses them using Spark, and load the data back into S3 as a set of dimensional tables and a fact table.

The collected data was processed and stored in the form of a star schema, allowing the business team to be able to analyze the songs heard by the customers using SQL queries.

<h1> Files description </h1>

The <code>etl.py</code> file contains functions that extract, transform and load parquet tables into bucket S3.

Log_data and song_data brings data for the purpose of testing locally.

Dockerfile allows build an container to run spark locally in an isolated environment.

<h1> Data description </h1>

Log_data brings information regarding the execution of songs by users in the application:

```javascript
{
    "artist":null,
    "auth":"Logged In",
    "firstName":"Celeste",
    "gender":"F",
    "itemInSession":1,
    "lastName":"Williams",
    "length":null,
    "level":"free",
    "location":"Klamath Falls, OR",
    "method":"GET",
    "page":"Home",
    "registration":1541077528796.0,
    "sessionId":52,
    "song":null,
    "status":200,
    "ts":1541207123796,
    "userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/37.0.2062.103 Safari\/537.36\"",
    "userId":"53"
}
```
Song_data brings information regarding each music (song_id, title, duration, year) and its respective aritsts (artist_id, artist_longitude, artist_latitude, artist_location, artist_name)

```javascript
{
    "num_songs": 1,
    "artist_id": "ARMJAGH1187FB546F3",
    "artist_latitude": 35.14968,
    "artist_longitude": -90.04892,
    "artist_location": "Memphis, TN",
    "artist_name": "The Box Tops",
    "song_id": "SOCIWDW12A8C13D406",
    "title": "Soul Deep",
    "duration": 148.03546,
    "year": 1969
    }
```
The instances of these two objects were extracted from S3, transformed and loaded into S3, using pyspark.

<h1> Data Model </h1>

time - DIM related to time;

users - DIM related to app users;

artists - DIM related to the artists of the songs that played in the app;

songs - DIM related to the songs played in the app;

songplays - FACT related to the playing songs in the app;

<h1> Running the Pipeline </h1>

<h2> AWS </h2>

1 - Include KEY and SECRET of AWS in <code>dwh.cfg</code>;

2 - Run <code>etl.py</code>.

<h2> Locally </h2>

1 - Install docker;

2 - Run docker build -t pyspark-notebook-name;

3 - Run docker run -p 8888:8888 pyspark-notebook-name;

4 - Click url to open jupyter-lab;

5 - Switch input_data and output_data to '' and <code>etl.py</code>.