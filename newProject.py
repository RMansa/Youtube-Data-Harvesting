from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import psycopg2
import pandas as pd
import streamlit as st
from PIL import Image

#API connection

def Api_connect():
    Api_Id="AIzaSyBnmt7oSzpadm9Ct57xN7jTqdQ-mV-9AQk"
    
    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()


def channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    if "items" in response and response["items"]:
        first_item = response["items"][0]
        data = dict(
            Channel_Name=first_item["snippet"]["title"],
            Channel_Id=first_item["id"],
            Subscribers=first_item["statistics"]["subscriberCount"],
            Views=first_item["statistics"]["viewCount"],
            Total_Videos=first_item["statistics"]["videoCount"],
            Channel_Description=first_item["snippet"]["description"],
            Playlist_Id=first_item["contentDetails"]["relatedPlaylists"]["uploads"]
        )
        return data

    return None  


def videos_ids(channel_id):
    video_ids = []

    response = youtube.channels().list(
        id=channel_id,
        part="contentDetails"
    ).execute()

    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        video_ids.extend(
            [item['snippet']['resourceId']['videoId'] for item in response1.get('items', [])]
        )

        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids


def video_info(video_ids):
    video_data = []

    request = youtube.videos().list(
        part='snippet,contentDetails,statistics',
        id=','.join(video_ids)  
    )
    response = request.execute()

    video_data = [
        {
            'Channel_Name': item['snippet']['channelTitle'],
            'Channel_Id': item['snippet']['channelId'],
            'Video_Id': item['id'],
            'Title': item['snippet']['title'],
            'Tags': item['snippet'].get('tags'),
            'Thumbnail': item['snippet']['thumbnails']['default']['url'],
            'Description': item['snippet'].get('description'),
            'Published_Date': item['snippet']['publishedAt'],
            'Duration': item['contentDetails']['duration'],
            'Views': item['statistics'].get('viewCount'),
            'Likes': item['statistics'].get('likeCount'),
            'Comments': item['statistics'].get('commentCount'),
            'Favorite_Count': item['statistics']['favoriteCount'],
            'Definaition': item['contentDetails']['definition'],
            'Caption_Status': item['contentDetails']['caption']
        }
        for item in response.get("items", [])
    ]

    return video_data



def comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response["items"]:
                data=dict(Comment_Id=item["snippet"]["topLevelComment"]["id"],
                        Video_id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                        Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        Comment_Published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                
                Comment_data.append(data)
    except:
        pass
    return Comment_data


def playlist_details(channel_id):
    play_data = []
    next_page_token = None

    for _ in range(5):
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        play_data.extend([
            {
                'Playlist_Id': item["id"],
                'Title': item["snippet"]["title"],
                'Channel_Id': item["snippet"]["channelId"],
                'Channel_Name': item["snippet"]["channelTitle"],
                'PublishedAt': item["snippet"]["publishedAt"],
                'Video_Count': item["contentDetails"]["itemCount"]
            }
            for item in response.get("items", [])
        ])

        next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break

    return play_data


#data transfer to MongoDb
client=pymongo.MongoClient("mongodb://localhost:27017")
mr=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=channel_info(channel_id)
    vi_ids=videos_ids(channel_id)
    vi_details=video_info(vi_ids)
    com_details=comment_info(vi_ids)
    pl_details=playlist_details(channel_id)

    coll1=mr["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed"

#Connect to SQL(Table creation for channels)

def channel_tab():
    mymr=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Fluffy",
                        database="youtube_data",
                        port="5432")
    step=mymr.cursor()

    drop_query='''drop table if exists channels'''
    step.execute(drop_query)
    mymr.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80),
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
        step.execute(create_query)
        mymr.commit()

    except:
        print("table already created")


    ch_list=[]
    mr=client["Youtube_data"]
    coll1=mr["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name ,
                                            Channel_Id ,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        try:
            step.execute(insert_query,values)
            mymr.commit()
        
        except:
            print("channels are already available")



#Table for playlists

def playlist_tab():
    mymr=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Fluffy",
                        database="youtube_data",
                        port="5432")
    step=mymr.cursor()

    drop_query='''drop table if exists playlists'''
    step.execute(drop_query)
    mymr.commit()


    create_query='''create table if not exists playlists(Playlist_Id varchar(100),
                                                        Title varchar(100) ,
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int)'''
    step.execute(create_query)
    mymr.commit()

    play_list=[]
    mr=client["Youtube_data"]
    coll1=mr["channel_details"]
    for play_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(play_data['playlist_information'])):
            play_list.append(play_data['playlist_information'][i])
    df1=pd.DataFrame(play_list)


    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id ,
                                            Title ,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_Count)
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_Count'])
        
        step.execute(insert_query,values)
        mymr.commit()



#Table for videos

def video_tab():
    mymr=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Fluffy",
                        database="youtube_data",
                        port="5432")
    step=mymr.cursor()

    drop_query='''drop table if exists videos'''
    step.execute(drop_query)
    mymr.commit()


    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(50),
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definaition varchar(10),
                                                        Caption_Status varchar(50))'''
    step.execute(create_query)
    mymr.commit()


    vi_list=[]
    mr=client["Youtube_data"]
    coll1=mr["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=pd.DataFrame(vi_list)


    for index,row in df2.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_Count,
                                            Definaition,
                                            Caption_Status)
                                            
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definaition'],
                row['Caption_Status'])
        
        step.execute(insert_query,values)
        mymr.commit()

    

#Table for comments

def comment_tab():
    mymr=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Fluffy",
                        database="youtube_data",
                        port="5432")
    step=mymr.cursor()

    drop_query='''drop table if exists comments'''
    step.execute(drop_query)
    mymr.commit()
    

    create_query='''create table if not exists comments(Comment_Id varchar(100),
                                                        Video_id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp)'''
    step.execute(create_query)
    mymr.commit()


    comment_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for comment_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(comment_data['comment_information'])):
            comment_list.append(comment_data['comment_information'][i])
    df3=pd.DataFrame(comment_list)


    for index,row in df3.iterrows():
        insert_query='''insert into comments(Comment_Id ,
                                            Video_id ,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published)
                                            
                                            values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
                row['Video_id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published'])
        
        step.execute(insert_query,values)
        mymr.commit()




def tables():
    channel_tab()
    video_tab()
    playlist_tab()
    comment_tab()

    return "success"



def info_ch_table():
    ch_list=[]
    mr=client["Youtube_data"]
    coll1=mr["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df


def info_pl_table():
    play_list=[]
    mr=client["Youtube_data"]
    coll1=mr["channel_details"]
    for play_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(play_data['playlist_information'])):
            play_list.append(play_data['playlist_information'][i])
    df1=st.dataframe(play_list)

    return df1


def info_vi_table():
    vi_list=[]
    mr=client["Youtube_data"]
    coll1=mr["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=st.dataframe(vi_list)

    return df2


def info_com_table():
    comment_list=[]
    mr=client["Youtube_data"]
    coll1=mr["channel_details"]
    for comment_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(comment_data['comment_information'])):
            comment_list.append(comment_data['comment_information'][i])
    df3=st.dataframe(comment_list)

    return df3



import requests
from io import BytesIO

image_url = "https://logos-world.net/wp-content/uploads/2020/04/YouTube-Emblem.png"


response = requests.get(image_url)
image = Image.open(BytesIO(response.content))
st.sidebar.image(image,use_column_width=True)

# Set the background color and text color using HTML and CSS
background_style = """
    <style>
        body {
            background-color: #f0f0f0; /* Set your desired background color */
            color: #333333; /* Set your desired text color */
            font-family: Arial, sans-serif; /* Set your desired font family */
        }

        .header {
            color: #FF0000; /* Red color for the header */
            padding: 20px;
            text-align: center;
            font-size: 36px;
            font-weight: bold;
        }

        .subheader {
            color: #FF0000; /* Red color for the subheader */
            text-align: center;
            font-size: 24px;
            margin-bottom: 20px;
        }

        .button {
            background-color: #FF0000; /* Red color for the button */
            color: #FFFFFF; /* White text color for the button */
            font-weight: bold;
            padding: 10px 20px;
            margin-top: 10px;
        }

        .success-message {
            color: #008000; /* Green color for success messages */
            font-weight: bold;
        }

        .radio-button {
            color: #FF0000; /* Red color for the radio button text */
        }
    </style>
"""
st.markdown(background_style, unsafe_allow_html=True)

# Header
st.markdown(
    '<div style="font-size: 36px; font-weight: bold; color: #ff0000; border-bottom: 2px solid #ffffff; padding-bottom: 8px;text-align: center;">YOUTUBE DATA HARVESTING AND WAREHOUSING</div>',
    unsafe_allow_html=True
)



# Sidebar Radio Button
show_tab_side = st.sidebar.radio("Main Menu", ("Home", "Data Collection", "SQL", "Insights"), key="main_menu", help="Choose a menu option")

import streamlit as st

# Function to display project overview
def project_overview():
    st.markdown("# Overview")
    st.write("Our project focuses on collecting YouTube data through the YouTube API, leveraging an API key for authentication. The collected data, encompassing information about channels, playlists, videos, and comments, is initially stored in MongoDB to accommodate its unstructured nature. To enhance data structure and enable more sophisticated analysis, we implement a migration process, transferring the data from MongoDB to SQL. This approach allows for a seamless transition from unstructured to structured data, facilitating comprehensive insights into the YouTube content landscape. The project utilizes a stack of tools, including Python for scripting, Pandas for data manipulation, the YouTube API for data collection, MongoDB for flexible storage, SQL (via PostGres) for structured analysis, and Streamlit for creating an interactive web application.")

    st.markdown("## Tools Used")
    st.write("I have utilized the following tools for data collection, analysis, and visualization:")
    st.write("- Python")
    st.write("- Pandas for data manipulation")
    st.write("- MongoDB for unstructured data storage and retrieval")
    st.write("- SQL for database queries")
    st.write("- Streamlit for creating the interactive web application")


if show_tab_side == "Home":
    project_overview()  


elif show_tab_side == "Data Collection":
    # Data Collection Form
    st.subheader(":red[Data Collection]")
    channel_id = st.text_input(":red[Channel ID for Data Collection]", key="channel_id_input", help="Enter the Channel ID for data collection")
    
    # Collect Data Button
    if st.button("Collect Data", key="collect_data_button"):
        ch_ids = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        

        for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        
       
        if channel_id in ch_ids:
            st.warning("Channel details for the given Channel ID already exist")
        else:
            insert = channel_details(channel_id)
            st.success(insert)

elif show_tab_side == "SQL":
    # SQL Migration Section
    st.subheader(":red[SQL Migration]")

    st.markdown(
            '<div style="background-color: rgba(255, 182, 193, 0.5); padding: 8px; border-radius: 8px;">Migrating data to SQL will store the collected data in a SQL database.</div>',
    unsafe_allow_html=True)


    ch_name = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_name.append(ch_data["channel_information"]["Channel_Name"])
    unique_channel_names = ch_name

    # Select box for choosing a channel name
    selected_channel = st.selectbox("Select Channel for Data Migration", unique_channel_names, key="channel_select")

    if st.button("Migrate Data to SQL Database", key="migrate_to_sql_button", help="Click to initiate data migration"):
        tabs = tables()
        st.success(tabs)
    
    
#Query connection

mymr=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Fluffy",
                        database="youtube_data",
                        port="5432")
step=mymr.cursor()

if show_tab_side=="Insights":
    ques=st.selectbox(":red[Here is the dropdown for Questions]",("1. What are the names of all the videos and their corresponding channels",
                                                            "2. Which channels have the most number of videos, and how many videos do they have",
                                                            "3. What are the top 10 most viewed videos and their respective channels",
                                                            "4. What are the number of comments and name of their corresponding videos",
                                                            "5. Which video has highest number of likes and their corresponding channel name",
                                                            "6. What are the total number of likes and dislikes and their corresponding video name",
                                                            "7. What is the total number of views for each channel and their corresponding channel name",
                                                            "8. What are the names of channels that are published videos in the year 2022",
                                                            "9. What is the avg duration of all videos and their corresponding channel name",
                                                            "10. Which video has the highest number of comments and their corresponding channel name"))


    if ques=="1. What are the names of all the videos and their corresponding channels":
        ques1='''select title as videos,channel_name as channelname from videos'''
        step.execute(ques1)
        mymr.commit()

        tab1=step.fetchall()
        df=pd.DataFrame(tab1,columns=["Video Title","Channel Name"])
        st.write(df)


    elif ques=="2. Which channels have the most number of videos, and how many videos do they have":
        ques2='''select channel_name as channelname,total_videos as num_of_videos from channels 
                    order by total_videos desc'''
        step.execute(ques2)
        mymr.commit()

        tab2=step.fetchall()
        df1=pd.DataFrame(tab2,columns=["Channel name","Num of Videos"])
        st.write(df1)


    elif ques=="3. What are the top 10 most viewed videos and their respective channels":
        ques3='''select views as views, channel_name as channelname, title as videotitle from videos 
                    where views is not null order by views desc limit 10'''
        step.execute(ques3)
        mymr.commit()

        tab3=step.fetchall()
        df2=pd.DataFrame(tab3,columns=["Views","Channel Name","Video Title"])
        st.write(df2)


    elif ques=="4. What are the number of comments and name of their corresponding videos":
        ques4='''select comments as num_of_comments, title as videotitle from videos 
                    where comments is not null'''
        step.execute(ques4)
        mymr.commit()

        tab4=step.fetchall()
        df3=pd.DataFrame(tab4,columns=["Comments","Video Title"])
        st.write(df3)


    elif ques=="5. Which video has highest number of likes and their corresponding channel name":
        ques5='''select title as videotitle,channel_name as channelname,likes as likescount from videos 
                    where likes is not null order by likes desc'''
        step.execute(ques5)
        mymr.commit()

        tab5=step.fetchall()
        df4=pd.DataFrame(tab5,columns=["Video Title","Channel Name","LikesCount"])
        st.write(df4)


    elif ques=="6. What are the total number of likes and dislikes and their corresponding video name":
        ques6='''select likes as likecount,title as videotitle from videos'''
        step.execute(ques6)
        mymr.commit()

        tab6=step.fetchall()
        df5=pd.DataFrame(tab6,columns=["LikesCount","Video Title"])
        st.write(df5)


    elif ques=="7. What is the total number of views for each channel and their corresponding channel name":
        ques7='''select views as views,channel_name as channelname from channels'''
        step.execute(ques7)
        mymr.commit()

        tab7=step.fetchall()
        df6=pd.DataFrame(tab7,columns=["Views","Channel Name"])
        st.write(df6)


    elif ques=="8. What are the names of channels that are published videos in the year 2022":
        ques8='''select title as videotitle,published_date as published,channel_name as channelname from videos
        where extract(year from published_date)=2022'''
        step.execute(ques8)
        mymr.commit()

        tab8=step.fetchall()
        df7=pd.DataFrame(tab8,columns=["Video Title","Published Date","Channel Name"])
        st.write(df7)


    elif ques=="9. What is the avg duration of all videos and their corresponding channel name":
        ques9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
        step.execute(ques9)
        mymr.commit()

        tab9=step.fetchall()
        df8=pd.DataFrame(tab9,columns=["Channel Name","Average duration"])

        Tab9=[]
        for index,row in df8.iterrows():
            Channel_title=row["Channel Name"]
            average_duration=row["Average duration"]
            avergae_duration_str=str(average_duration)
            Tab9.append(dict(channeltitle=Channel_title,avgdur=avergae_duration_str))
        df9=pd.DataFrame(Tab9)
        st.write(df9)


    elif ques=="10. Which video has the highest number of comments and their corresponding channel name":
        ques10='''select title as videotitle,channel_name as channelname, comments as comments from videos
                where comments is not null order by comments desc'''
        step.execute(ques10)
        mymr.commit()

        tab10=step.fetchall()
        df10=pd.DataFrame(tab10,columns=["Video Title","Channel Name","Comments"])
        st.write(df10)
