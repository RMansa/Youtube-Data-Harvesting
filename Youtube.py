from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import psycopg2
import pandas as pd
import streamlit as st


#API connection

def Api_connect():
    Api_Id="AIzaSyBnmt7oSzpadm9Ct57xN7jTqdQ-mV-9AQk"
    
    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()


#Channel info
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response["items"]:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i["statistics"]["subscriberCount"],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data


#video ID

def get_videos_ids(channel_id):
  video_ids=[]

  response=youtube.channels().list(id=channel_id,
                                  part="contentDetails").execute()
  Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  next_page_token=None

  while True:
    response1=youtube.playlistItems().list(
                                      part='snippet',
                                      playlistId=Playlist_Id,
                                      maxResults=50,
                                      pageToken=next_page_token).execute()

    for i in range(len(response1['items'])):
      video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    next_page_token=response1.get('nextPageToken')

    if next_page_token is None:
      break
  return video_ids


#Video Info

def get_video_info(video_ids):
    video_data=[]

    for video_id in video_ids:
      request=youtube.videos().list(
          part='snippet,contentDetails,statistics',
          id=video_ids
      )
      response=request.execute()

      for item in response["items"]:
        data=dict(Channel_Name=item['snippet']['channelTitle'],
                  Channel_Id=item['snippet']['channelId'],
                  Video_Id=item['id'],
                  Title=item['snippet']['title'],
                  Tags=item['snippet'].get('tags'),
                  Thumbnail=item['snippet']['thumbnails']['default']['url'],
                  Description=item['snippet'].get('description'),
                  Published_Date=item['snippet']['publishedAt'],
                  Duration=item['contentDetails']['duration'],
                  Views=item['statistics'].get('viewCount'),
                  Likes=item['statistics'].get('likeCount'),
                  Comments=item['statistics'].get('commentCount'),
                  Favorite_Count=item['statistics']['favoriteCount'],
                  Definaition=item['contentDetails']['definition'],
                  Caption_Status=item['contentDetails']['caption']
                  )
        video_data.append(data)
    return video_data 


#Comment info
def get_comment_info(video_ids):
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



# Playlist info

def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part="snippet,contentDetails",
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response["items"]:
                        data=dict(Playlist_Id=item["id"],
                                Title=item["snippet"]["title"],
                                Channel_Id=item["snippet"]["channelId"],
                                Channel_Name=item["snippet"]["channelTitle"],
                                PublishedAt=item["snippet"]["publishedAt"],
                                Video_Count=item["contentDetails"]["itemCount"])
                        All_data.append(data)


                next_page_token=response.get("nextPageToken")
                if next_page_token is None:
                        break
        return All_data




#Data transfer to Mongodb
client=pymongo.MongoClient("mongodb+srv://mansamlaahl2019:Fluffy@cluster0.z7prsu2.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_data"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    pl_details=get_playlist_details(channel_id)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed"

#Connect to SQL(Table creation for channels)

def channel_tab():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Fluffy",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80),
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("table already created")


    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
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
            cursor.execute(insert_query,values)
            mydb.commit()
        
        except:
            print("channels are already available")



#Table for playlists

def playlist_tab():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Fluffy",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists playlists(Playlist_Id varchar(100),
                                                        Title varchar(100) ,
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int)'''
    cursor.execute(create_query)
    mydb.commit()

    play_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
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
        
        cursor.execute(insert_query,values)
        mydb.commit()



#Table for videos

def video_tab():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Fluffy",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


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
    cursor.execute(create_query)
    mydb.commit()


    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
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
        
        cursor.execute(insert_query,values)
        mydb.commit()


#Table for comments

def comment_tab():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Fluffy",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments(Comment_Id varchar(100),
                                                        Video_id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp)'''
    cursor.execute(create_query)
    mydb.commit()


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
        
        cursor.execute(insert_query,values)
        mydb.commit()


def tables():
    channel_tab()
    video_tab()
    playlist_tab()
    comment_tab()

    return "success"
    


def info_ch_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df


def info_pl_table():
    play_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for play_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(play_data['playlist_information'])):
            play_list.append(play_data['playlist_information'][i])
    df1=st.dataframe(play_list)

    return df1


def info_vi_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=st.dataframe(vi_list)

    return df2


def info_com_table():
    comment_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for comment_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(comment_data['comment_information'])):
            comment_list.append(comment_data['comment_information'][i])
    df3=st.dataframe(comment_list)

    return df3



#Streamlit

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Technologies/Tools-")
    st.caption("Python scripting-Visual studio code")
    st.caption("API Integration")
    st.caption("MongoDB")
    st.caption("Data management through SQL-postgres")
    
   
channel_id=st.text_input("Channel ID")

if st.button("Collect Data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    if channel_id in ch_ids:
        st.success("Channel details for given Channel Id already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Tabs=tables()
    st.success(Tabs)

show_tab=st.radio("Table for view",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_tab=="CHANNELS":
    info_ch_table()

elif show_tab=="PLAYLISTS":
    info_pl_table()

elif show_tab=="VIDEOS":
    info_vi_table()

elif show_tab=="COMMENTS":
    info_com_table()


#Query connection

mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Fluffy",
                        database="youtube_data",
                        port="5432")
cursor=mydb.cursor()

ques=st.selectbox("Here is the dropdown for Questions",("1. What are the names of all the videos and their corresponding channels",
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
    cursor.execute(ques1)
    mydb.commit()

    tab1=cursor.fetchall()
    df=pd.DataFrame(tab1,columns=["Video Title","Channel Name"])
    st.write(df)


elif ques=="2. Which channels have the most number of videos, and how many videos do they have":
    ques2='''select channel_name as channelname,total_videos as num_of_videos from channels 
                order by total_videos desc'''
    cursor.execute(ques2)
    mydb.commit()

    tab2=cursor.fetchall()
    df1=pd.DataFrame(tab2,columns=["Channel name","Num of Videos"])
    st.write(df1)


elif ques=="3. What are the top 10 most viewed videos and their respective channels":
    ques3='''select views as views, channel_name as channelname, title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(ques3)
    mydb.commit()

    tab3=cursor.fetchall()
    df2=pd.DataFrame(tab3,columns=["Views","Channel Name","Video Title"])
    st.write(df2)


elif ques=="4. What are the number of comments and name of their corresponding videos":
    ques4='''select comments as num_of_comments, title as videotitle from videos 
                where comments is not null'''
    cursor.execute(ques4)
    mydb.commit()

    tab4=cursor.fetchall()
    df3=pd.DataFrame(tab4,columns=["Comments","Video Title"])
    st.write(df3)


elif ques=="5. Which video has highest number of likes and their corresponding channel name":
    ques5='''select title as videotitle,channel_name as channelname,likes as likescount from videos 
                where likes is not null order by likes desc'''
    cursor.execute(ques5)
    mydb.commit()

    tab5=cursor.fetchall()
    df4=pd.DataFrame(tab5,columns=["Video Title","Channel Name","LikesCount"])
    st.write(df4)


elif ques=="6. What are the total number of likes and dislikes and their corresponding video name":
    ques6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(ques6)
    mydb.commit()

    tab6=cursor.fetchall()
    df5=pd.DataFrame(tab6,columns=["LikesCount","Video Title"])
    st.write(df5)


elif ques=="7. What is the total number of views for each channel and their corresponding channel name":
    ques7='''select views as views,channel_name as channelname from channels'''
    cursor.execute(ques7)
    mydb.commit()

    tab7=cursor.fetchall()
    df6=pd.DataFrame(tab7,columns=["Views","Channel Name"])
    st.write(df6)


elif ques=="8. What are the names of channels that are published videos in the year 2022":
    ques8='''select title as videotitle,published_date as published,channel_name as channelname from videos
    where extract(year from published_date)=2022'''
    cursor.execute(ques8)
    mydb.commit()

    tab8=cursor.fetchall()
    df7=pd.DataFrame(tab8,columns=["Video Title","Published Date","Channel Name"])
    st.write(df7)


elif ques=="9. What is the avg duration of all videos and their corresponding channel name":
    ques9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(ques9)
    mydb.commit()

    tab9=cursor.fetchall()
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
    cursor.execute(ques10)
    mydb.commit()

    tab10=cursor.fetchall()
    df10=pd.DataFrame(tab10,columns=["Video Title","Channel Name","Comments"])
    st.write(df10)
