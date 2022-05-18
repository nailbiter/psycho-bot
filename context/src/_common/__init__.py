"""===============================================================================

        FILE: _common.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION:

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION:
     VERSION: ---
     CREATED: 2022-04-03T22:08:04.028205
    REVISION: ---

==============================================================================="""
import logging
import os
import requests
import json
import re
from datetime import datetime, timedelta
import pandas as pd
import io
#import uuid
import pymongo
from jinja2 import Template
import randomname


def get_psycho_transitions(fn):
    df = pd.read_json(fn)
    return df


def get_mongo_client(collname):
    client = pymongo.MongoClient(os.environ["MONGO_URL"])
    coll = client.psychobot[collname]
    return client, coll


def get_random_uuid():
    #    return str(uuid.uuid4())
    return randomname.generate().replace("-", "_")
