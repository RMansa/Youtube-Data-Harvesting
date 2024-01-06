## Project Descriptions:

- The problem statement is to create a Streamlit application that allows users to access and analyze data from __Multiple YouTube Channels__:
   
   - Ability to input a _**YouTube channel ID**_ and retrieve all the relevant data using _**Google API**_.
 
        __| Channel name | Subscribers | Total video count | Playlist ID | Video ID | Likes| Comments of each video |__
     
   - Option to store the data in a MongoDB database as a data lake.
   - Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
   - Option to select a channel name and migrate its data from the data lake to a SQL database as tables.
   - Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.


## Basic Requirements:

- __[Python 3.11](https://www.google.com/search?q=docs.python.org)__
- __[googleapiclient](https://www.google.com/search?q=googleapiclient+python)__ 
- __[mysql_connector](https://www.google.com/search?q=mysql+connector)__ 
- __[Pandas](https://www.google.com/search?q=python+pandas)__
- __[Streamlit](https://www.google.com/search?q=python+streamlit)__
- __[Numpy](https://www.google.com/search?q=numpy)__ 
- __[pymongo](https://www.google.com/search?q=pymongo)__
- __[requests](https://www.google.com/search?q=requests)__


## General BackEnd WorkFlow Of This Project:
1.__Api Call And Data Sorting:__

  - _Based On The Users Need, Users Can Fetch Data From YouTube By Entering Url or Keyword_ 
  - To Make This Work I Designed Two Separate Files To Make Api Calls **_Single_Channel.py_** and **_Multi_Channel.py_** using **_Googleapiclient_**
      > which is inside off __Yapi__ Directory Of This Repo
  - After Data Got Fetched it is Shown as Three Separate Section Chanenl Details, Video Details, Comments Details
  - For Sorting And Isolation of Values From Data I have Used **_Pandas_** 
  - For Visualize The Data I Had Used **_Streamlit_** Inbuilt markdown features along with html 
  
2.__Uploading To MongoDb Atlas:__
    
  - Api call Gets Data in _JSON_ Format with lots of Details in each catagories:[Youtube Docs](https://developers.google.com/youtube/v3/docs/)
  
  ``` py
                    1. Channels
                    2. Videos 
                    3. CommentThreads
                    4. Search and many more
  ```
  
  - Data get Formated and Made Ready for Users to Upload to MongoDB which is **_Data Lake_** 
  - In MongoDB Each users Data is Stored in DB Called `youtube` and Collections name is Created based upon on the users Channel search
  - Sample of Data are shown to Users in **_Streamlit_** App After Succesfull Insert of Data into _[MongoDB Atlas](https://mongodb.com/)_

##### MongoDB Data Sample
  ``` json
  
                      {
           "_id":"ExamPro-Channel",
           "Channels_Data":[
              {
                 "Channel_id":"UC2EsmbKnDNE7y1N3nZYCuGw",
                 "Channel_Name":"ExamPro",
                 "Playlist_id":"UU2EsmbKnDNE7y1N3nZYCuGw",
                 "Created_Date":"2018-10-15T00:48:34Z",
                 "Subcribers":"27200",
                 "TotalViews":"3088251",
                 "TotalVideos":"2754",
                 "Thumbnail":"https://yt3.ggpht.com/Cp5qTPY5Riz_MkI-WgSShDIfddjKlO7NYpWu-uYABE7ghCHFuF2LGAPRovaJ8DNGxswIkWGv1Q=s240-c-k-c0x00ffffff-no-rj",
                 "Channel_link":"https://www.youtube.com/channel/UC2EsmbKnDNE7y1N3nZYCuGw"
              }
           ]
        }
  ```




3.__Uploading To postgresSQL DataBase:__

   - Data From MongoDB are then Converted into Tables and Rows using __Pandas__ with Normalization of Values are ready to Upload to MysqlDB
   - **_postgresSQL connector** is used for Connecting App and MysqlDB 
   - In Multiple Channel Mode Users Have Options of Choosing Channels That Needs to Be Uploaded to SQLDB

   
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

4.__Querying From sql DataBase:__

   - Querying Data From MysqlDB Have Two Options With **_Pre-Defined Query_** and **_Custom Query_*
   - **_Pre-Defined Query_** will display details of the Selected Channels or Channels that are currentely Shown in Channels List 
   - **_Custom Query_** will have options to use **_SQL_** Queries to get Details of all Channels, Videos, Comments Using all basic _SQL_ Commends.  

     
