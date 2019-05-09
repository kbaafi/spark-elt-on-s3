# ELT: Json to Parquet tables

The project demonstrates cloud based ELT data processing. The input data held in S3 is extracted into partitioned parquet tables and loaded back to S3.

## Dependencies

- configparser

- boto3

- pyspark

## Running the ELT

Running the pipeline consists of two processes:

1. Setup the input and output data sources

2. Edit the **dl-s3.cfg** file

3. Run ***python etls3.py*** to transform the Json data to Parquet and store on S3

## Data Sources

The input data consists of two datasets currently stored on AWS S3:

1. A subset of real song data from the Million Song Dataset: Each file is in JSON format and contains metadata about a song and the artist of that song. The metadata consists of fields such as song ID, artist ID, artist name, song title, artist location, etc. A sample song record is shown below:

    ```javascript
       {"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, 
    "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", 
        "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0} 
    ```

2. Logs of user activity on a purported music streaming application. The data is actually generated using an event generatory. This data captures user activity on the app and stores metadata such as artist name, authentication status, first name,length ,time , ect. A sample of the log data is shown below:

    ```javascript
        {"artist":"N.E.R.D. FEATURING MALICE","auth":"Logged In","firstName":"Jayden","gender":"M","itemInSession":0,"lastName":"Fox","length":288.9922,"level":"free",
    "location":"New Orleans-Metairie, LA","method":"PUT","page":"NextSong",
        "registration":1541033612796.0,"sessionId":184,"song":"Am I High (Feat. Malice)",
            "status":200,"ts":1541121934796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"101"}
    ```

## The database

### Fact Table(s)

**Songplays:** This table tracks the songs played by users on the platform. It tracks the following metadata:

```javascript
{song_play_id, session_id, user_id,start_time,artist_id,location,song_id, agent}
```

### Dimension Table(s)

**Users:** Maintains unique user data on users of the platform

**Artists:** Maintains unique artist metadata

**Songs:** Maintains unique song metadata

**Time:** Allows tracking of time at which user interactions with the platform occurs

## ELT Process

For each data store, the list of files is generated, and then upon read the necessary schema is applied to the data.

From the **Songs data** the following tables were extracted:

- songs

- artists

From the **logs data** the following tables were extracted:

- users

- time

The **songs data** and the **logs data** are joined to produce the songplays data

## Results

The results of the process can be confirmed by running the notebook **etl-test.ipynb**
