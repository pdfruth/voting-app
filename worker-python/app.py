#!/usr/bin/env python3

from redis import Redis
import os
import time
import psycopg2
import json

def get_redis():
   redishost = os.environ.get('REDIS_HOST', 'new-redis')
   print ("Connecting to Redis using " + redishost)
   #redis_conn = Redis(host=redishost, db=0, socket_timeout=5)
   redis_conn = Redis(host=redishost, db=0, socket_timeout=5, password='admin')
   redis_conn.ping()
   print ("connected to redis!") 
   return redis_conn

def connect_postgres(): 
   # Vva1VrSRCqqJnYKH
   host = os.getenv('POSTGRES_SERVICE_HOST')
   db_name = os.getenv('DB_NAME', "db") 
   db_user = os.getenv('DB_USER', "pfruth") 
   db_pass = os.getenv('DB_PASS', "pfruth") 
   print (db_name) 
   try:
      print ("connecting to the DB") 
      #conn = psycopg2.connect("host=db user=postgres password=dbp host=172.30.114.217")
      #conn = psycopg2.connect ("host={} dbname={} user={} password={}".format("sample-app", "postgres", "dave", "dave") )
      #conn = psycopg2.connect ("host={} dbname={} user={} password={}".format("new-postgresql", "postgres", "pfruth", "pfruth"))
      conn = psycopg2.connect ("host={} dbname={} user={} password={}".format("new-postgresql", db_name, db_user, db_pass))
      print ("Successfully connected to PostGres")
      
      cursor = conn.cursor()
      sqlCreateTable = "CREATE TABLE IF NOT EXISTS public.votes (id VARCHAR(255) NOT NULL, vote VARCHAR(255) NOT NULL);"
      cursor.execute(sqlCreateTable)
      print ("votes table created") 
      conn.commit()
      cursor.close()
      return conn 

   except Exception as e:
      print (e)

def insert_postgres(conn, data): 
    try: 
       cur = conn.cursor() 
       cur.execute("insert into votes values (%s, %s)",
       ( 
          data.get("voter_id"), 
          data.get("vote")
       ))
       conn.commit()  
       print ("row inserted into DB") 
       cur.close()

    except Exception as e: 
       conn.rollback()
       cur.close()
       print ("error inserting into postgres")  
       print (str(e)) 

def process_votes(db_conn):
    redis = get_redis()
    redis.ping()  
    while True: 
       try:  
          msg = redis.rpop("votes")
          print(msg)
          if (msg != None): 
             print ("reading message from redis")
             msg_dict = json.loads(msg)
             insert_postgres(db_conn, msg_dict) 
          # will look like this
          # {"vote": "a", "voter_id": "71f0caa7172a84eb"}
          time.sleep(3)        
   
       except Exception as e:
          print(e)

if __name__ == '__main__':
    db_conn = connect_postgres()
    process_votes(db_conn)
    db_conn.close()
