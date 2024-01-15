# YouTube Data Harvesting and Warehousing

## Project Description

The goal of this project is to create a Streamlit application that enables users to access and analyze data from multiple YouTube channels. The application provides the following functionalities:

1. **Data Retrieval from YouTube:**
   - Users can input a YouTube channel ID and retrieve relevant data using the Google API.
   - Data includes Channel name, Subscribers, Total video count, Playlist ID, Video ID, Likes, and Comments for each video.
  
  
     ``` py
                    1. Channels
                    2. Videos 
                    3. CommentThreads
                    4. Search and many more

2. **Storage in MongoDB:**
   - Users can store data in a MongoDB database acting as a data lake.
   - Data can be collected for up to 10 different YouTube channels and stored in the data lake with a single button click.
   ```html
         

         {'Channel_Name': 'ANTUBER',
          'Channel_Id': 'UCemjlI9CerTnnvqtt0hz9yw',
          'Subscribers': '39300',
          'Views': '22264932',
          'Total_Videos': '26',
          'Channel_Description': 'moved onto something even bigger lol\n- rr, ag\n',
          'Playlist_Id': 'UUemjlI9CerTnnvqtt0hz9yw'}


3. **Migration to SQL Database:**
   - Users have the option to select a channel name and migrate its data from the data lake to a SQL database as tables.

4. **SQL Database Interaction:**
   - Users can search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

    ``` sql
           
        CREATE TABLE IF NOT EXISTS Channel_Table (
            Ch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            Channel_Id VARCHAR(30) UNIQUE,
            Channel_Name VARCHAR(40),
            Playlist_Id VARCHAR(30),
            Created_Date DATETIME,
            Subscribers BIGINT,
            Total_Views BIGINT,
            Total_Videos BIGINT);

## General Requirements

- Python 3.11
   - Libraries: googleapiclient, postgresSQL, Pandas, Streamlit, Numpy, pymongo, requests.

- **Database:**
  - MongoDB: Data lake for storing raw data.
  - PostgreSQL: SQL database for structured data.

- **Streamlit App:**
  - Main Streamlit application file (not specified in the provided details).

## Conclusion

This Streamlit application provides users with a user-friendly interface to interact with YouTube data, store it in different databases, and perform data analysis. The integration of MongoDB and PostgreSQL ensures a flexible and structured approach to data warehousing. Users can easily navigate through the application to retrieve valuable insights from YouTube channels.
