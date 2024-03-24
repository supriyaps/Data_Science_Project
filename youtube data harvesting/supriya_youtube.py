import googleapiclient.discovery 
from pprint import pprint        #to print data structures in a readable, pretty way
import streamlit as st
# import webbrowser
# webbrowser.open('http://streamlit.io')
import pymongo
import psycopg2
import pandas as pd



#establish connect with yt
api_key ="AIzaSyCBIEZHpY7oXYtb-Gv6IUcjTUpPY485q-A"
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

# scarpping channel details 

def channel_info(channel_id):
   request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
   response = request.execute()
  
   z = dict(channel_id = response['items'][0]['id'],
            channel_name = response['items'][0]['snippet']['title'],
            subscriber_count = response['items'][0]['statistics']['subscriberCount'],
            channel_views = response['items'][0]['statistics']['viewCount'],
            total_videos  =  response['items'][0]['statistics']['videoCount'],
            description = response['items'][0]['snippet']['description'],
            Playlist_id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            )

   return z

# channel_id = "UC2J_VKrAzOEJuQvFFtj3KUw"
# details = channel_details('UC2J_VKrAzOEJuQvFFtj3KUw')
# pprint(details)



#get video ids

def get_video_ids(channel_id):
    video_ids=[]
    response= youtube.channels().list(id=channel_id,
                                        part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None 


    while True:
            
            response1  = youtube.playlistItems().list(
                    part='snippet',
                    playlistId=Playlist_Id,
                    maxResults=50,
                    pageToken=next_page_token 
                    ).execute()
            

            for i in range(len(response1['items'])):
                video_ids.append(response1['items'][0]['snippet']['resourceId']['videoId'])

            next_page_token=response1.get('nextPageToken')
            if next_page_token is None:
                break

           
    return  video_ids
# videoids= get_video_ids('UC2J_VKrAzOEJuQvFFtj3KUw')

# pprint(len(videoids))

#get video info
def get_video_info(Video_ids):
    video_data=[]
    # for video_id in videoids:
    for video_id in Video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id)
    response=request.execute()
    for item in response['items']:
        data = dict(channel_Name=response['items'][0]['snippet']['channelTitle'],
                    channel_id =response['items'][0]['snippet']['channelId'],
                    video_id=response['items'][0]['id'],
                    video_title=response['items'][0]['snippet']['title'],
                    video_tags=response['items'][0]['snippet'].get('tags'),
                    thumbnais=response['items'][0]['snippet']['thumbnails']['default']['url'],                                                   #tags can be null also hence we use get()
                    video_description=response['items'][0]['snippet'].get('description'),
                    published_date=response['items'][0]['snippet']['publishedAt'],
                    duration=response['items'][0]['contentDetails']['duration'],
                    views=response['items'][0]['statistics'].get('viewCount'),
                    commentscount=response['items'][0]['statistics']['commentCount'],
                    likecount=response['items'][0]['statistics'].get('likeCount'),
                    favoritecount=response['items'][0]['statistics']['favoriteCount'],
                    caption=response['items'][0]['contentDetails']['caption']
                    )    
        video_data.append(data)
    return video_data  
# video_details=get_video_info(videoids)


# pprint(get_video_info(videoids))


#get comment info
def get_comment_info(video_ids):
    comment_data=[]
    try:                        #to avoid error if comment is dissabled
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50

                )
            response = request.execute()

            for item in response['items']:
                    x=dict(
                        comment_id= (item['snippet']['topLevelComment']['id']),
                        video_id=(item['snippet']['topLevelComment']['snippet']['videoId']),
                        comment_text=(item['snippet']['topLevelComment']['snippet']['textDisplay']),
                        comment_author=(item['snippet']['topLevelComment']['snippet']['authorDisplayName']),
                        comment_published_on=(item['snippet']['topLevelComment']['snippet']['publishedAt'])
                        )
            comment_data.append(x)
            
    except:
        pass
    return  comment_data
# comment_detals=get_comment_info(videoids)
#pprint(comment_detals)


#get playlist details

def get_Playlist_details(channel_id):
    next_page_token=None
    playlist_data=[]
    while True:
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
                
            )
        response = request.execute()
        for items in response['items']:
            y=dict(
                
                playlistid=items['id'],
                title=items['snippet']['title'],
                channel_id=items['snippet']['channelId'],
                channel_name =items['snippet']['channelTitle']
            ) 

            playlist_data.append(y)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data

# pprint(len(get_Playlist_details('UC2J_VKrAzOEJuQvFFtj3KUw')))

#establishing connection in mongodb

client=pymongo.MongoClient('mongodb+srv://supri21031998:bdoOTlRo971yUPMD@cluster0.gonzvz8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db=client['Youtube_data']  #creating database

#uploading to mongo db

def channel_details(channel_id):
            ch_details=channel_info(channel_id)
            pl_details=get_Playlist_details(channel_id)
            vi_ids=get_video_ids(channel_id)
            vi_details=get_video_info(vi_ids)
            comment_details=get_comment_info(vi_ids)
            
            collection1=db["channel_Details"]     #creating collection
            collection1.insert_one({"channel_information":ch_details,"playlist_details":pl_details,
             
                                    "video_details":vi_details ,"comment_info":comment_details})          #insert in json format in mongodb
              

# insert= channel_details("UC2J_VKrAzOEJuQvFFtj3KUw") #csk

#insert= channel_details("UCCq1xDJMBRF61kiOgU90_kw")   #rcb  UC9kXOjMbTeDsoVTmhwmaPvg
#insert= channel_details("UC9kXOjMbTeDsoVTmhwmaPvg") 
# insert= channel_details("UCfuTUCHcXuKzruspkUVlD0g")
#insert= channel_details("UCFOgUdmcBDySJfGImF1avyQ")
#insert= channel_details("UC5cY198GU1MQMIPJgMkCJ_Q")
insert= channel_details("UCxBnuxjV8ZTMYVUHhXos0yw")

#table creation for the channels and the details

def channels_table():
        mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="root1",
                                database="youtube_data",
                                port="5432")
        cursor=mydb.cursor()

        # drop_query='''drop table if channel is already present '''
        # cursor.execute(drop_query)
        # mydb.commit()

        try:   #to avoid error
                create_query='''create table if not exists channels(channel_name varchar(100),
                                                                channel_id varchar(80) primary key,
                                                                subscriber_count bigint,
                                                                channel_views bigint,
                                                                total_videos int,
                                                                description text,
                                                                Playlist_id varchar(80))'''
                cursor.execute(create_query)
                mydb.commit()

        except:
                print("Channels table already created")


        ch_list=[]
        db=client["Youtube_data"]
        collection1=db["channel_Details"]
        for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
                ch_list.append(ch_data["channel_information"])
                #pprint(ch_list)
                df=pd.DataFrame(ch_list)
                #print(df)


                for index,row in df.iterrows():     # columns in postgressql
                        insert_query='''insert into  channels ( channel_name,
                                                                        channel_id,
                                                                        subscriber_count,
                                                                        channel_views,
                                                                        total_videos,
                                                                        description,
                                                                        Playlist_id)              
                                                                        
                                                                        
                                                                        
                                                                        values(%s,%s,%s,%s,%s,%s,%s)'''   
                
                values=(row['channel_name'],               
                                row['channel_id'],
                                row['subscriber_count'],
                                row['channel_views'],
                                row['total_videos'],
                                row['description'],
                                row['Playlist_id'])    # values in df
                


        try:
                cursor.execute(insert_query,values)
                mydb.commit()

        except:
                        print("Channel values have been inserted")
#playlist 
def playlist_table():
    mydb=psycopg2.connect(host="localhost",
                                    user="postgres",
                                    password="root1",
                                    database="youtube_data",
                                    port="5432")
    cursor=mydb.cursor()

    create_query='''create table if not exists playlists(playlistid varchar(100) primary key,
                                                            title varchar(100),
                                                            channel_id varchar(100),
                                                            channel_name varchar(100)
                                                            )'''

    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["Youtube_data"]
    collection1=db["channel_Details"]
    for pl_data in collection1.find({},{"_id":0,"playlist_details":1}):
            for i in range(len(pl_data["playlist_details"])):
              pl_list.append(pl_data["playlist_details"][i])

    df1=pd.DataFrame(pl_list)           

    for index,row in df1.iterrows():     # columns in postgressql
                            insert_query='''insert into  playlists ( playlistid,
                                                                            title,
                                                                            channel_id,
                                                                            channel_name
                                                                                    
                                                                            )
                                                                            
                                                                            
                                                                            values(%s,%s,%s,%s)'''   
                    
    values=(                        row['playlistid'],
                                    row['title'],
                                    row['channel_id'],
                                    row['channel_name']
           )
                                    
                      
    cursor.execute(insert_query,values)
    mydb.commit()


def videos_tables():
    mydb=psycopg2.connect(host="localhost",
                                        user="postgres",
                                        password="root1",
                                        database="youtube_data",
                                        port="5432")
    cursor=mydb.cursor()

    create_query='''create table if not exists videos(channel_Name varchar(100),
                                                        channel_id varchar(100),
                                                        video_id varchar(20) primary key,
                                                        video_title varchar(150),
                                                        video_tags text,
                                                        thumbnais varchar(200),                                                 
                                                        video_description text,
                                                        published_date timestamp,
                                                        duration interval,
                                                        views bigint,
                                                        commentscount int,
                                                        likecount bigint,
                                                        favoritecount int,
                                                        caption varchar(100)
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["Youtube_data"]
    collection1=db["channel_Details"]
    for vi_data in collection1.find({},{"_id":0,"video_details":1}):
            for i in range(len(vi_data["video_details"])):
                vi_list.append(vi_data["video_details"][i])

    df1=pd.DataFrame(vi_list)           

    for index,row in df1.iterrows():     # columns in postgressql
                            insert_query='''insert into  videos ( channel_Name,
                                                        channel_id,
                                                        video_id,
                                                        video_title,
                                                        video_tags,
                                                        thumbnais,                                                 
                                                        video_description,
                                                        published_date,
                                                        duration,
                                                        views,
                                                        commentscount,
                                                        likecount,
                                                        favoritecount,
                                                        caption
                                                                                    
                                                                            )
                                                                            
                                                                            
                                                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''   
                    
    values=(                        row['channel_Name'],
                                    row['channel_id'],
                                    row['video_id'],
                                    row['video_title'],
                                    row['video_tags'],
                                    row['thumbnais'],
                                    row['video_description'],
                                    row['published_date'],
                                    row['duration'],
                                    row['views'],
                                    row['commentscount'],
                                    row['likecount'],
                                    row['favoritecount'],
                                    row['caption']
                                    )
                                
                    
    cursor.execute(insert_query,values)
    mydb.commit()
    #comment table
def comment_tables():
        mydb=psycopg2.connect(host="localhost",
                                        user="postgres",
                                        password="root1",
                                        database="youtube_data",
                                        port="5432")
        cursor=mydb.cursor()

        create_query='''create table if not exists comments(comment_id varchar(100) primary key,
                                                        video_id varchar(50),
                                                        comment_text   text,
                                                        comment_author varchar(150),
                                                        comment_published_on timestamp
                                                        )'''

        cursor.execute(create_query)
        mydb.commit()

        cd_list=[]
        db=client["Youtube_data"]
        collection1=db["channel_Details"]
        for com_data in collection1.find({},{"_id":0,"comment_info":1}):
                for i in range(len(com_data["comment_info"])):
                  cd_list.append(com_data["comment_info"][i])

        df3=pd.DataFrame(cd_list)           

        for index,row in df3.iterrows():     # columns in postgressql
                                insert_query='''insert into  comments(comment_id,
                                                                        video_id,
                                                                        comment_text,
                                                                        comment_author,
                                                                        comment_published_on
                                                                                        
                                                                )
                                                                                
        
                                                                values(%s,%s,%s,%s,%s)'''   

                
        values=(row['comment_id'],
                row['video_id'],
                row['comment_text'],
                row['comment_author'],
                row['comment_published_on']

                )
                                
                        
        cursor.execute(insert_query,values)
        mydb.commit()
def tables(channel_name):

    data= channels_table(channel_name)
    if data:
        st.write(data)
    else:
        playlist_table(channel_name)
        videos_tables(channel_name)
        comment_tables(channel_name)

    return "Tables Created Successfully"

# def tables():
#     channels_table()
#     playlist_table()
#     videos_tables()
#     comment_tables()
#     return  "tables created"
# Tables= tables()

def display_channel_table():
    ch_list=[]
    db=client["Youtube_data"]
    collection1=db["channel_Details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
                ch_list.append(ch_data["channel_information"])
                #pprint(ch_list)
    df=st.dataframe(ch_list)
    return df


def display_playlists_table():
   pl_list=[]
   db=client["Youtube_data"]
   collection1=db["channel_Details"]
   for pl_data in collection1.find({},{"_id":0,"playlist_details":1}):
            for i in range(len(pl_data["playlist_details"])):
              pl_list.append(pl_data["playlist_details"][i])

   df1=st.dataframe(pl_list)  
   return df1

def display_video_table():
    vi_list=[]
    db=client["Youtube_data"]
    collection1=db["channel_Details"]
    for vi_data in collection1.find({},{"_id":0,"video_details":1}):
            for i in range(len(vi_data["video_details"])):
                vi_list.append(vi_data["video_details"][i])
    df2=st.dataframe(vi_list)
    return df2

def display_comment_table():
        cd_list=[]
        db=client["Youtube_data"]
        collection1=db["channel_Details"]
        for com_data in collection1.find({},{"_id":0,"comment_info":1}):
                for i in range(len(com_data["comment_info"])):
                  cd_list.append(com_data["comment_info"][i])

        df3=st.dataframe(cd_list)     
        return df3

#streamlit code
#Header 
st.header('YouTube Data Harvesting and Warehousing using SQL and Streamlit ', divider='rainbow')
st.header('_submited by supriya_')

channel_id=st.text_input('input channel id:')
if st.button("collect data"):
    all_channels= []
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        all_channels.append(ch_data["channel_information"]["Channel_Name"])
            
    unique_channel= st.selectbox("Select the Channel",all_channels)

    if st.button("migration to sql"):
     Table=tables(unique_channel)
     st.success(Table)

show_table=st.radio("SELECT THE TABLE TO VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
        display_channel_table()

elif show_table=="PLAYLISTS":
    display_playlists_table()

elif show_table=="VIDEOS":
    display_video_table()

elif show_table=="COMMENTS":
    display_comment_table()




#establishing sql connection

mydb=psycopg2.connect(host="localhost",
                                        user="postgres",
                                        password="root1",
                                        database="youtube_data",
                                        port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. What are the names of all the videos and their corresponding channels?",
                                              "2. Which channels have the most number of videos, and how many videos do they have?",
                                              "3. What are the top 10 most viewed videos and their respective channels?",
                                              "4. How many comments were made on each video, and what are their corresponding video names?",
                                              "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8. What are the names of all the channels that have published videos in the year 2022?",
                                              "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question=="1. What are the names of all the videos and their corresponding channels?":
    query1='''select video_title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name ","total videos"])
    st.write(df2)
    
elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3='''select views as views,channel_name as channelname,video_title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)
elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4='''select commentscount as all_comments,video_title as videotitle from videos where commentscount is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select video_title as videotitle,channel_Name as channel_Name,likecount as likecount
                from videos where likecount is not null order by likecount desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6='''select likecount as likecounts,video_title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecounts","videotitle"])
    st.write(df6)
elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query7='''select channel_name as channelname ,channel_views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
    query8='''select video_title as video_title,published_date as videorelease,channel_Name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select channel_Name as channelname,AVG(duration) as averageduration from videos group by channel_Name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)
elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select video_title as videotitle, channel_Name as channelname,commentscount as comments from videos where commentscount is
                not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)
    
    

