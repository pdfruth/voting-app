#!/usr/bin/env python3

from redis import Redis

def get_redis():
    if not hasattr(g, 'redis'):
        g.redis = Redis(host="redis", db=0, socket_timeout=5, password=os.getenv('redispasswd', "password"))
    return g.redis


def process_votes():
    try:  
        redis = get_redis()
        msg = redis.get("votes")
        print(msg)        
   
    except Exception as e:
        print(e)


if __name__ == '__main__':
    process_votes()