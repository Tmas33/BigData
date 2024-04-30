import requests
import yaml
import redis
import csv
from redis.commands.json.path import Path
from pybaseball import  playerid_lookup
from pybaseball import statcast
from pybaseball import  statcast_pitcher
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

#loading in config parameters from the yaml file
def load_config():
    """Load configuration from the YAML file.

    Returns:
        dict: Configuration data.
    """
    with open("config.yaml", "r") as file:
        return yaml.safe_load(file)
    
config = load_config()

#connecting to the redis DB
def get_redis_connection():
    """Create a Redis connection using the configuration.

    Returns:
        Redis: Redis connection object.
    """
    return redis.Redis(
        host=config["redis"]["host"],
        port=config["redis"]["port"],
        db=0,
        decode_responses=True,
        username=config["redis"]["user"],
        password=config["redis"]["password"],
    )

#method to clear DB out
def flushDB():
    r = get_redis_connection()
    r.flushall()

r = get_redis_connection()

"""
One set of data collected using the following:
# collect Statcast data on all pitches from the months of May and June
data = statcast('2023-05-01', '2023-05-05')
data.shape

data2 = data.dropna(subset=['launch_angle', 'launch_speed', 'estimated_ba_using_speedangle'])
data2.shape

print(data2)
#data2.to_csv("baseball.csv")

"""

pitcher = playerid_lookup('bauer', 'trevor')

# His MLBAM ID is 543037, so we feed that as the player_id argument to the following function 
bauer_stats = statcast_pitcher('2012-04-01', '2021-10-25', 545333)

#bauer_stats.to_csv("baseball.csv")

dataFile = 'baseball.csv'

r.close()
flushDB()

with open(dataFile,mode='r') as f:
    data = csv.DictReader(f)
    for line in data:
        Index_id = line.pop('Index')
        r.hmset(f"index:{Index_id}",line)
        print(f"{Index_id} added to db")

r.close()

