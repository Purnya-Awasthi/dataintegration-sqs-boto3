"""
This module creates database, db extension, queues, table, and calculates
5 closest landmarks to a given latitude and longitude and also send that
information to queues
"""


import boto3
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
try:
   #connecting to boto3
   sqs = boto3.resource('sqs',aws_access_key_id =  '',
                        aws_secret_access_key = '')
   print(sqs)

   queue = sqs.create_queue(QueueName='pwtc-datastream2', Attributes={'DelaySeconds': '5'})
   print(queue)
   #creating and connecting to databse postgis using psycopg2
   try:
        connection = psycopg2.connect(dbname="postgis", user="purnya", password="purnya",host="localhost")
        print("connection")
   except(Exception, psycopg2.Error) as error:
       print(error)
   connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
   cursor = connection.cursor()


   #creating the  databse
   cursor.execute("drop database if exists ptwc_postgis;")

   create_database = """create database ptwc_postgis; """
   cursor.execute(create_database)
   connection.commit()

   #create extension postgis
   create_extension_query = """create extension if not exists postgis;"""
   cursor.execute(create_extension_query)
   connection.commit()

   #drop table is exists
   cursor.execute("drop table landmarks")
   #create tables and indexes
   create_tables_landmarks = """  CREATE TABLE landmarks
(
  gid character varying(5) NOT NULL,
  name character varying(50),
  address character varying(50),
  date_built character varying(10),
  architect character varying(50),
  landmark character varying(10),
  latitude double precision,
  longitude double precision,
  the_geom geometry,
  CONSTRAINT landmarks_pkey PRIMARY KEY (gid),
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_geom CHECK (geometrytype(the_geom) = 'POINT'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4326)
);
"""
   cursor.execute(create_tables_landmarks)
   connection.commit()
   create_index_landmarks = """ CREATE INDEX if not exists landmarks_the_geom_gist ON landmarks USING gist (the_geom )"""
   cursor.execute(create_index_landmarks)
   connection.commit()

   #insertion of data
   insert_data = """ copy landmarks(name,gid,address,date_built,architect,landmark,latitude,longitude) FROM '/home/unhm-128-07/Desktop/comp851/finalproject/Individual_Landmarks.csv' DELIMITERS ',' CSV HEADER """
   cursor.execute(insert_data)
   connection.commit()

   #sending insertion info to queue
   response = queue.send_message(MessageBody='Landmarks',MessageAttributes={
      'Insertion':{
         'StringValue':'Data is uploaded',
         'DataType':'String'
         }})

   queue = sqs.get_queue_by_name(QueueName='pwtc-datastream2')

   #Translate latitude and longitude into POINT geometry
   update_query = """UPDATE landmarks SET the_geom = ST_GeomFromText('POINT(' || longitude || ' ' || latitude || ')',4326) """
   cursor.execute(update_query)
   connection.commit()

   #5 closest landmarks to a given latitude and longitude
   select_query = """SELECT distinct
ST_Distance(ST_GeomFromText('POINT(-87.6348345 41.8786207)', 4326), landmarks.the_geom) AS planar_degrees,
name,
architect, latitude, longitude
FROM landmarks
ORDER BY planar_degrees ASC
LIMIT 5 """
   count = 1
   cursor.execute(select_query)
   connection.commit()
   location_details=[]
   rows = cursor.fetchall()
   print("5 closest landmarks to -87.6348345 41.8786207")
   print("*******************")
   for row in rows:
       print("Location-" + str(count))
       print("----------")
       print("Planar_Degrees - " + str(row[0]))
       print("Name - " + str(row[1]))
       print("Architect - " + str(row[2]))
       print("Latitude - "+ str(row[3]))
       print("Longitude - "+ str(row[4]))
       print("*******************")
       count +=1
       location_details.append(str(row[0]))
       location_details.append(str(row[1]))
       location_details.append(str(row[2]))
       location_details.append(str(row[3]))
       location_details.append(str(row[4]))

   #sending notification of location data to the queue
   response = queue.send_message(MessageBody='Landmarks',MessageAttributes={
      'Locations':{
         'StringValue':",".join(location_details),
         'DataType':'String'
         }})
   connection.commit()


except (Exception, psycopg2.Error) as error :
    if(connection):
        print(error)

finally:
    #close all connections
    if(connection):
        cursor.close()
        connection.close()
        print("DB connection is closed")
